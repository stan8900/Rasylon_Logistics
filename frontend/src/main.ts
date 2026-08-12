import "./styles.css";

type TelegramUser = {
  id?: number;
  first_name?: string;
  last_name?: string;
  username?: string;
};

type TelegramWebApp = {
  initData: string;
  initDataUnsafe?: { user?: TelegramUser };
  ready: () => void;
  expand: () => void;
  openTelegramLink: (url: string) => void;
  HapticFeedback?: {
    notificationOccurred: (type: "success" | "warning" | "error") => void;
    impactOccurred: (style: "light" | "medium" | "heavy") => void;
  };
};

declare global {
  interface Window {
    Telegram?: { WebApp?: TelegramWebApp };
  }
}

type LocationSignal = {
  id: string;
  name: string;
  country: string;
  lat: number;
  lon: number;
  drivers: number;
  messages: number;
  updated_at: string;
  trend: number[];
  subscribed: boolean;
  favorite: boolean;
};

type DriverActivity = {
  id: string;
  driver: string;
  username: string;
  location: string;
  destination: string;
  vehicle_type: string;
  availability: string;
  confidence: number;
  message: string;
  source: string;
  minutes_ago: number;
};

type MiniConfig = {
  payment: {
    amount_text: string;
    card_target: string;
  };
  bot: {
    url: string | null;
    support: string;
  };
};

const API_BASE = (import.meta.env.VITE_API_BASE_URL || "https://rasylonlogistics-production.up.railway.app").replace(/\/$/, "");
const tg = window.Telegram?.WebApp;

if (tg) {
  tg.ready();
  tg.expand();
}

let config: MiniConfig | null = null;
let activeScreen = "card";
let isAuthenticated = false;
let browserLoginToken: string | null = null;
let browserLoginPoll: number | null = null;
let selectedLocationId = "tashkent";
let radiusKm = 120;
let query = "";
let locations: LocationSignal[] = [
  {
    id: "tashkent",
    name: "Ташкент",
    country: "Узбекистан",
    lat: 41.31,
    lon: 69.28,
    drivers: 18,
    messages: 42,
    updated_at: "2 мин назад",
    trend: [8, 11, 13, 16, 18],
    subscribed: true,
    favorite: true,
  },
  {
    id: "samarkand",
    name: "Самарканд",
    country: "Узбекистан",
    lat: 39.65,
    lon: 66.96,
    drivers: 9,
    messages: 21,
    updated_at: "8 мин назад",
    trend: [4, 6, 5, 8, 9],
    subscribed: false,
    favorite: true,
  },
  {
    id: "almaty",
    name: "Алматы",
    country: "Казахстан",
    lat: 43.24,
    lon: 76.9,
    drivers: 14,
    messages: 37,
    updated_at: "5 мин назад",
    trend: [10, 9, 12, 13, 14],
    subscribed: true,
    favorite: false,
  },
  {
    id: "bishkek",
    name: "Бишкек",
    country: "Кыргызстан",
    lat: 42.87,
    lon: 74.59,
    drivers: 7,
    messages: 16,
    updated_at: "14 мин назад",
    trend: [2, 3, 5, 7, 7],
    subscribed: false,
    favorite: false,
  },
];

let activities: DriverActivity[] = [
  {
    id: "a1",
    driver: "Rasul",
    username: "@rasul_tir",
    location: "Ташкент",
    destination: "Алматы",
    vehicle_type: "фура тент",
    availability: "сегодня",
    confidence: 0.94,
    source: "gruz_uz",
    minutes_ago: 2,
    message: "Стою Ташкент, фура тент, ищу груз на Алматы, готов сегодня",
  },
  {
    id: "a2",
    driver: "Aziz",
    username: "@aziz_ref",
    location: "Самарканд",
    destination: "Ташкент",
    vehicle_type: "рефрижератор",
    availability: "утром",
    confidence: 0.89,
    source: "gruzoperevozky_sng",
    minutes_ago: 8,
    message: "Самарканд, реф, свободен, ищу загрузку на завтра",
  },
  {
    id: "a3",
    driver: "Bek",
    username: "@bek_log",
    location: "Алматы",
    destination: "Ташкент",
    vehicle_type: "изотерм",
    availability: "сейчас",
    confidence: 0.91,
    source: "logistics_kazakhstan",
    minutes_ago: 11,
    message: "Алматы стою, изотерм, направление Ташкент/Шымкент",
  },
];

const app = document.querySelector<HTMLDivElement>("#app");
if (!app) throw new Error("App root not found");

app.innerHTML = `
  <main class="app-shell">
    <header class="topbar">
      <button class="icon-button" data-screen-target="card" aria-label="Визитка">ID</button>
      <div class="brand">
        Rasylon Logistics
        <span>live-карта водителей из Telegram</span>
      </div>
      <button class="icon-button" data-action="support" aria-label="Поддержка">?</button>
    </header>

    <section class="screen active" data-screen="dashboard">
      <section class="editorial-hero">
        <div class="hero-rule"><span></span></div>
        <p>Telegram intelligence / driver locations</p>
        <h1>DRIVERS</h1>
        <div class="hero-copy">
          <strong>Live activity map</strong>
          <span>Водители появляются из сообщений Telegram-групп после parser, AI classification и geocoding.</span>
        </div>
      </section>

      <section class="live-map" aria-label="Карта активности водителей">
        <div class="map-grid"></div>
        <div class="map-road road-main"></div>
        <div class="map-road road-alt"></div>
        <div class="map-toolbar">
          <label>
            <span>Локация</span>
            <input id="locationSearch" placeholder="Ташкент, Самарканд..." />
          </label>
          <label>
            <span>Радиус</span>
            <input id="radiusInput" type="range" min="20" max="500" step="20" value="120" />
          </label>
        </div>
        <div class="radius-label"><strong id="radiusLabel">120 км</strong><span>поиск активности</span></div>
        <div id="mapPins"></div>
      </section>

      <section class="live-summary inverted">
        <div>
          <span>Сейчас ищут груз</span>
          <strong id="totalDrivers">0</strong>
        </div>
        <div>
          <span>Telegram-сообщения</span>
          <strong id="totalMessages">0</strong>
        </div>
        <div>
          <span>Уверенность AI</span>
          <strong>91%</strong>
        </div>
      </section>

      <section class="section-head compact">
        <h1>Активность водителей</h1>
        <button data-screen-target="locations">Локации</button>
      </section>
      <div class="activity-list" id="activityList"></div>
    </section>

    <section class="screen" data-screen="locations">
      <div class="section-head">
        <h1>Локации</h1>
        <button id="refreshSignals">Обновить</button>
      </div>
      <div class="search-row">
        <input id="locationsFilter" placeholder="Город, хаб, склад, граница" />
        <button class="icon-button" id="clearLocations" aria-label="Очистить">×</button>
      </div>
      <div class="location-list" id="locationList"></div>
    </section>

    <section class="screen" data-screen="messages">
      <section class="editorial-strip">
        <span>Source</span>
        <strong>Telegram messages become map signals.</strong>
      </section>
      <div class="section-head">
        <h1>Telegram сигналы</h1>
        <button id="simulateMessage">+ сигнал</button>
      </div>
      <div class="parser-flow">
        <span>Telegram</span><span>Parser</span><span>AI</span><span>Geo</span><span>Dashboard</span>
      </div>
      <div class="activity-list" id="messageList"></div>
    </section>

    <section class="screen" data-screen="login">
      <div class="login-shell">
        <section class="visit-card compact">
          <div class="avatar large">RL</div>
          <h1>Вход по Telegram</h1>
          <p>Введите номер, который привязан к Telegram. Код придет от нашего бота.</p>
        </section>

        <form class="panel login-panel" id="otpForm">
          <h2>Подтверждение номера</h2>
          <button class="primary-button wide" type="button" id="openBotLogin">Войти через Telegram-бота</button>
          <div class="divider"><span>или OTP по номеру</span></div>
          <div class="otp-grid">
            <label>Телефон<input id="authPhone" inputmode="tel" autocomplete="tel" placeholder="+998..." /></label>
            <button class="secondary-button" type="button" id="sendOtp">Получить OTP</button>
            <label>OTP<input id="authOtp" inputmode="numeric" autocomplete="one-time-code" placeholder="123456" /></label>
            <button class="primary-button" type="submit">Перейти на dashboard</button>
          </div>
          <p class="hint">Если бот еще не связан с номером, откройте Telegram-бота и нажмите /start.</p>
          <div class="status" id="otpStatus"></div>
        </form>
      </div>
    </section>

    <section class="screen" data-screen="profile">
      <section class="editorial-strip">
        <span>Account</span>
        <strong>Все настройки, оплата и подписка находятся здесь.</strong>
      </section>
      <div class="profile-card">
        <div class="avatar" id="avatar">RL</div>
        <div>
          <h1 id="profileName">Rasylon Logistics</h1>
          <p id="profileMeta">Telegram не привязан</p>
        </div>
      </div>

      <div class="panel">
        <h2>Визитка</h2>
        <div class="settings-list">
          <div><span>Telegram</span><strong id="telegramValue">@rasylon</strong></div>
          <div><span>Компания</span><strong>Rasylon Logistics</strong></div>
          <div><span>Телефон</span><strong id="phoneValue">не указан</strong></div>
        </div>
      </div>

      <div class="panel">
        <h2>Подписка и оплата</h2>
        <div class="settings-list">
          <div><span>Тариф</span><strong>Pro мониторинг</strong></div>
          <div><span>Статус</span><strong>активен</strong></div>
          <div><span>Сумма</span><strong id="amountText">...</strong></div>
          <div><span>Карта</span><strong id="cardText">...</strong></div>
        </div>
        <form id="paymentForm" class="inline-pay">
          <label>Номер Telegram<input id="telegramPhone" inputmode="tel" autocomplete="tel" placeholder="+998..." /></label>
          <label>Карта отправителя<input id="cardNumber" inputmode="numeric" autocomplete="cc-number" placeholder="8600 ...." /></label>
          <label>Имя на карте<input id="cardName" autocomplete="cc-name" placeholder="Имя Фамилия" /></label>
          <button class="primary-button wide" type="submit">Отправить оплату на проверку</button>
          <div class="status" id="payStatus"></div>
        </form>
      </div>

      <div class="profile-actions">
        <button class="secondary-button wide" id="openBot">Открыть Telegram-бота</button>
        <button class="ghost-button wide" id="logoutButton">Выйти</button>
      </div>
    </section>

    <section class="screen" data-screen="card">
      <div class="visit-card">
        <div class="avatar large">RL</div>
        <div class="visit-copy">
          <span class="eyebrow">Telegram-бот для логистики и авторассылок</span>
          <h1>Одно сообщение — десятки Telegram-групп автоматически.</h1>
          <p>Автоматизируйте рассылку грузов, маршрутов и предложений по Telegram-группам из одного бота.</p>
        </div>
        <div class="feature-cloud">
          <span>Авторассылка по выбранным группам</span>
          <span>Текст и интервалы отправки</span>
          <span>Управление группами</span>
          <span>Проверка доступности чатов</span>
          <span>Баланс и история оплат</span>
          <span>Автодоступ после оплаты</span>
          <span>Статистика отправок и ошибок</span>
          <span>Админ-панель</span>
          <span>Отдельный Telegram-аккаунт для массовой рассылки</span>
        </div>
        <div class="flow-card">
          <strong>Как это работает</strong>
          <p>Пополняете баланс → создаёте сообщение → выбираете Telegram-группы → запускаете авторассылку.</p>
          <p>Бот отправляет сообщения по расписанию и сообщает, если какая-либо группа стала недоступна.</p>
        </div>
        <div class="audience-grid">
          <span>Водители</span>
          <span>Диспетчеры</span>
          <span>Логистические компании</span>
          <span>Экспедиторы и перевозчики</span>
        </div>
        <button class="primary-button wide" data-screen-target="login">Войти по номеру</button>
      </div>
    </section>
  </main>

  <nav class="bottom-nav">
    <button class="nav-item active" data-screen-target="dashboard">Карта</button>
    <button class="nav-item" data-screen-target="locations">Локации</button>
    <button class="nav-item" data-screen-target="messages">Сообщения</button>
    <button class="nav-item" data-screen-target="profile">Профиль</button>
  </nav>
`;

function byId<T extends HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) throw new Error(`Missing element: ${id}`);
  return element as T;
}

function basePayload(): Record<string, unknown> {
  return {
    tg_init_data: tg?.initData || "",
    telegram_user: tg?.initDataUnsafe?.user || null,
  };
}

function setStatus(element: HTMLElement, message: string, ok = false): void {
  element.classList.toggle("ok", ok);
  element.textContent = message;
}

function setScreen(screen: string): void {
  const protectedScreens = ["dashboard", "locations", "messages", "profile"];
  if (!isAuthenticated && protectedScreens.includes(screen)) {
    screen = "login";
  }
  activeScreen = screen;
  document.body.classList.toggle("is-authenticated", isAuthenticated);
  document.querySelectorAll<HTMLElement>(".screen").forEach((element) => {
    element.classList.toggle("active", element.dataset.screen === screen);
  });
  document.querySelectorAll<HTMLButtonElement>(".nav-item").forEach((button) => {
    button.classList.toggle("active", button.dataset.screenTarget === screen);
  });
  window.scrollTo({ top: 0, behavior: "smooth" });
}

async function postJson<T>(path: string, payload: Record<string, unknown>): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "request_failed");
  return data as T;
}

function selectedLocation(): LocationSignal {
  return locations.find((location) => location.id === selectedLocationId) || locations[0];
}

function filteredLocations(): LocationSignal[] {
  const search = query.trim().toLowerCase();
  if (!search) return locations;
  return locations.filter((location) => `${location.name} ${location.country}`.toLowerCase().includes(search));
}

function locationPosition(location: LocationSignal): { left: number; top: number } {
  const minLat = 38.8;
  const maxLat = 44.2;
  const minLon = 65.8;
  const maxLon = 78.1;
  const left = ((location.lon - minLon) / (maxLon - minLon)) * 74 + 12;
  const top = (1 - (location.lat - minLat) / (maxLat - minLat)) * 64 + 16;
  return { left: Math.max(8, Math.min(86, left)), top: Math.max(10, Math.min(82, top)) };
}

function renderMap(): void {
  const pins = byId<HTMLDivElement>("mapPins");
  pins.innerHTML = locations
    .map((location) => {
      const position = locationPosition(location);
      const active = location.id === selectedLocationId ? "active" : "";
      return `
        <button class="map-pin ${active}" data-location-id="${location.id}" style="left:${position.left}%;top:${position.top}%">
          <strong>${location.drivers}</strong>
          <span>${location.name}</span>
        </button>
      `;
    })
    .join("");
  byId("radiusLabel").textContent = `${radiusKm} км`;
  byId("totalDrivers").textContent = String(locations.reduce((sum, location) => sum + location.drivers, 0));
  byId("totalMessages").textContent = String(locations.reduce((sum, location) => sum + location.messages, 0));
}

function renderActivities(): void {
  const current = selectedLocation().name;
  const relevant = activities.filter((activity) => activity.location === current || activeScreen === "messages");
  const html = relevant
    .map((activity) => `
      <article class="activity-card">
        <div class="activity-head">
          <div>
            <strong>${activity.driver}</strong>
            <span>${activity.username} · ${activity.minutes_ago} мин назад</span>
          </div>
          <em>${Math.round(activity.confidence * 100)}%</em>
        </div>
        <p>${activity.message}</p>
        <div class="chips">
          <span>${activity.location}</span>
          <span>→ ${activity.destination}</span>
          <span>${activity.vehicle_type}</span>
          <span>${activity.availability}</span>
        </div>
        <footer>Источник: Telegram-группа @${activity.source}</footer>
      </article>
    `)
    .join("");
  byId("activityList").innerHTML = html || `<div class="empty">В этой локации пока нет уверенных сигналов.</div>`;
  byId("messageList").innerHTML = activities.map((activity) => `
    <article class="activity-card source">
      <div class="activity-head"><strong>@${activity.source}</strong><em>${Math.round(activity.confidence * 100)}%</em></div>
      <p>${activity.message}</p>
      <div class="chips"><span>driver_searching_cargo</span><span>${activity.location}</span><span>${activity.vehicle_type}</span></div>
    </article>
  `).join("");
}

function renderLocations(): void {
  byId<HTMLDivElement>("locationList").innerHTML = filteredLocations()
    .map((location) => `
      <article class="location-card ${location.id === selectedLocationId ? "active" : ""}" data-location-id="${location.id}">
        <div>
          <strong>${location.name}</strong>
          <span>${location.country} · обновлено ${location.updated_at}</span>
        </div>
        <div class="location-stats">
          <span>${location.drivers} водителей</span>
          <span>${location.messages} сообщений</span>
        </div>
        <div class="spark">${location.trend.map((value) => `<i style="height:${10 + value * 2}px"></i>`).join("")}</div>
        <div class="card-actions">
          <button data-toggle-sub="${location.id}">${location.subscribed ? "Отписаться" : "Подписаться"}</button>
          <button data-toggle-fav="${location.id}">${location.favorite ? "В избранном" : "В избранное"}</button>
        </div>
      </article>
    `)
    .join("");
}

async function loadConfig(): Promise<void> {
  const response = await fetch(`${API_BASE}/api/mini/config`);
  config = await response.json() as MiniConfig;
  byId("amountText").textContent = config.payment.amount_text;
  byId("cardText").textContent = config.payment.card_target;
}

async function loadSignals(): Promise<void> {
  try {
    const response = await fetch(`${API_BASE}/api/mini/locations`);
    if (!response.ok) return;
    const data = await response.json() as { locations?: LocationSignal[]; activities?: DriverActivity[] };
    if (Array.isArray(data.locations) && data.locations.length) locations = data.locations;
    if (Array.isArray(data.activities) && data.activities.length) activities = data.activities;
  } finally {
    renderAll();
  }
}

async function submitPayment(event: SubmitEvent): Promise<void> {
  event.preventDefault();
  const status = byId<HTMLDivElement>("payStatus");
  setStatus(status, "");
  try {
    const phone = byId<HTMLInputElement>("telegramPhone").value;
    await postJson("/api/mini/payment", {
      ...basePayload(),
      telegram_phone: phone,
      whatsapp_phone: phone,
      card_number: byId<HTMLInputElement>("cardNumber").value,
      card_name: byId<HTMLInputElement>("cardName").value,
    });
    tg?.HapticFeedback?.notificationOccurred("success");
    setStatus(status, "Оплата отправлена админам на проверку.", true);
  } catch (error) {
    tg?.HapticFeedback?.notificationOccurred("error");
    const code = error instanceof Error ? error.message : "";
    setStatus(status, code === "phone_required" ? "Укажите Telegram номер." : code === "bad_card" ? "Проверьте карту." : "Не удалось отправить оплату.");
  }
}

async function sendOtp(): Promise<void> {
  const status = byId<HTMLDivElement>("otpStatus");
  setStatus(status, "");
  if (!tg?.initData) {
    await startBrowserLogin();
    return;
  }
  try {
    await postJson("/api/auth/request-otp", { ...basePayload(), phone: byId<HTMLInputElement>("authPhone").value });
    setStatus(status, "OTP отправлен в Telegram-бот. Код действует 5 минут.", true);
  } catch (error) {
    const code = error instanceof Error ? error.message : "";
    setStatus(status, code === "telegram_not_linked" ? "Номер пока не связан с Telegram. Откройте бота и нажмите /start." : "Не удалось отправить OTP. Попробуйте позже.");
  }
}

async function startBrowserLogin(): Promise<void> {
  const status = byId<HTMLDivElement>("otpStatus");
  setStatus(status, "");
  try {
    const response = await postJson<{ token: string; bot_url: string | null }>("/api/auth/browser-login/start", {});
    browserLoginToken = response.token;
    if (browserLoginPoll !== null) window.clearInterval(browserLoginPoll);
    browserLoginPoll = window.setInterval(() => void checkBrowserLogin(), 1800);
    setStatus(status, "Откройте Telegram-бота и нажмите Start. После подтверждения dashboard откроется автоматически.", true);
    if (response.bot_url) {
      window.open(response.bot_url, "_blank", "noopener,noreferrer");
    }
  } catch {
    setStatus(status, "Не удалось создать ссылку для входа. Попробуйте позже.");
  }
}

async function checkBrowserLogin(): Promise<void> {
  if (!browserLoginToken) return;
  try {
    const response = await fetch(`${API_BASE}/api/auth/browser-login/check?token=${encodeURIComponent(browserLoginToken)}`);
    const data = await response.json() as { status?: string; telegram_user?: TelegramUser };
    if (data.status === "confirmed") {
      if (browserLoginPoll !== null) window.clearInterval(browserLoginPoll);
      browserLoginPoll = null;
      browserLoginToken = null;
      isAuthenticated = true;
      const userInfo = data.telegram_user;
      if (userInfo) {
        const name = [userInfo.first_name, userInfo.last_name].filter(Boolean).join(" ") || userInfo.username || "Telegram user";
        byId("profileName").textContent = name;
        byId("profileMeta").textContent = userInfo.username ? `@${userInfo.username}` : `ID ${userInfo.id}`;
        byId("telegramValue").textContent = userInfo.username ? `@${userInfo.username}` : `ID ${userInfo.id}`;
      }
      setScreen("dashboard");
      return;
    }
    if (data.status === "expired" && browserLoginPoll !== null) {
      window.clearInterval(browserLoginPoll);
      browserLoginPoll = null;
      browserLoginToken = null;
      setStatus(byId<HTMLDivElement>("otpStatus"), "Ссылка истекла. Запросите вход через Telegram заново.");
    }
  } catch {
    // Keep polling; short network failures should not cancel the login attempt.
  }
}

async function verifyOtp(event: SubmitEvent): Promise<void> {
  event.preventDefault();
  const status = byId<HTMLDivElement>("otpStatus");
  try {
    const phone = byId<HTMLInputElement>("authPhone").value;
    await postJson("/api/auth/verify-otp", { phone, otp: byId<HTMLInputElement>("authOtp").value });
    isAuthenticated = true;
    byId("phoneValue").textContent = phone;
    byId("profileMeta").textContent = "Telegram подтвержден";
    tg?.HapticFeedback?.notificationOccurred("success");
    setStatus(status, "Вход выполнен.", true);
    setScreen("dashboard");
  } catch (error) {
    const code = error instanceof Error ? error.message : "";
    setStatus(status, code === "otp_expired" ? "OTP истек. Запросите новый код." : code === "too_many_attempts" ? "Слишком много попыток. Попробуйте позже." : "Неверный OTP.");
  }
}

function simulateSignal(): void {
  const newActivity: DriverActivity = {
    id: `a${Date.now()}`,
    driver: "Driver",
    username: "@new_driver",
    location: "Ташкент",
    destination: "Алматы",
    vehicle_type: "тент",
    availability: "сейчас",
    confidence: 0.87,
    source: "gruz_uz",
    minutes_ago: 0,
    message: "Ташкент стою, тент, ищу груз на Алматы",
  };
  activities = [newActivity, ...activities];
  const tashkent = locations.find((location) => location.id === "tashkent");
  if (tashkent) {
    tashkent.drivers += 1;
    tashkent.messages += 1;
    tashkent.updated_at = "только что";
  }
  renderAll();
}

function renderAll(): void {
  renderMap();
  renderActivities();
  renderLocations();
}

document.addEventListener("click", (event) => {
  const target = event.target as HTMLElement;
  const screenButton = target.closest<HTMLElement>("[data-screen-target]");
  if (screenButton?.dataset.screenTarget) {
    setScreen(screenButton.dataset.screenTarget);
    renderActivities();
    return;
  }
  const locationButton = target.closest<HTMLElement>("[data-location-id]");
  if (locationButton?.dataset.locationId) {
    selectedLocationId = locationButton.dataset.locationId;
    setScreen(activeScreen === "locations" ? "dashboard" : activeScreen);
    renderAll();
    return;
  }
  const subscribeButton = target.closest<HTMLElement>("[data-toggle-sub]");
  if (subscribeButton?.dataset.toggleSub) {
    const location = locations.find((item) => item.id === subscribeButton.dataset.toggleSub);
    if (location) location.subscribed = !location.subscribed;
    renderLocations();
    return;
  }
  const favButton = target.closest<HTMLElement>("[data-toggle-fav]");
  if (favButton?.dataset.toggleFav) {
    const location = locations.find((item) => item.id === favButton.dataset.toggleFav);
    if (location) location.favorite = !location.favorite;
    renderLocations();
  }
});

byId<HTMLInputElement>("radiusInput").addEventListener("input", (event) => {
  radiusKm = Number((event.target as HTMLInputElement).value);
  renderMap();
});
byId<HTMLInputElement>("locationSearch").addEventListener("input", (event) => {
  query = (event.target as HTMLInputElement).value;
  renderLocations();
});
byId<HTMLInputElement>("locationsFilter").addEventListener("input", (event) => {
  query = (event.target as HTMLInputElement).value;
  renderLocations();
});
byId<HTMLButtonElement>("clearLocations").addEventListener("click", () => {
  query = "";
  byId<HTMLInputElement>("locationsFilter").value = "";
  renderLocations();
});
byId<HTMLButtonElement>("refreshSignals").addEventListener("click", () => void loadSignals());
byId<HTMLButtonElement>("simulateMessage").addEventListener("click", simulateSignal);
byId<HTMLButtonElement>("openBotLogin").addEventListener("click", () => void startBrowserLogin());
byId<HTMLButtonElement>("sendOtp").addEventListener("click", () => void sendOtp());
byId<HTMLFormElement>("otpForm").addEventListener("submit", (event) => void verifyOtp(event as SubmitEvent));
byId<HTMLFormElement>("paymentForm").addEventListener("submit", (event) => void submitPayment(event as SubmitEvent));
byId<HTMLButtonElement>("openBot").addEventListener("click", () => {
  const url = config?.bot.url || "https://t.me/atRasylon_bot";
  tg ? tg.openTelegramLink(url) : window.location.assign(url);
});
byId<HTMLButtonElement>("logoutButton").addEventListener("click", () => {
  isAuthenticated = false;
  byId("profileMeta").textContent = "Telegram не привязан";
  byId("phoneValue").textContent = "не указан";
  setScreen("card");
});
document.querySelector<HTMLElement>('[data-action="support"]')?.addEventListener("click", () => {
  const support = (config?.bot.support || "@rasylon_support").replace("@", "");
  const url = `https://t.me/${support}`;
  tg ? tg.openTelegramLink(url) : window.location.assign(url);
});

const user = tg?.initDataUnsafe?.user;
if (user) {
  const name = [user.first_name, user.last_name].filter(Boolean).join(" ") || user.username || "Telegram user";
  byId("profileName").textContent = name;
  byId("profileMeta").textContent = user.username ? `@${user.username}` : "Telegram WebApp";
  byId("telegramValue").textContent = user.username ? `@${user.username}` : `ID ${user.id}`;
}

renderAll();
void loadConfig();
void loadSignals();
setScreen(activeScreen);
