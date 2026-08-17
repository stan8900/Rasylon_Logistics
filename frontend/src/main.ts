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
    ymaps3?: any;
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
  intent?: string;
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
  maps?: {
    yandex_api_key?: string | null;
  };
};

type BrowserLoginStartResponse = {
  token: string;
  bot_url: string | null;
  expires_in?: number;
};

type MessageClassification = {
  intent: string;
  current_location: string | null;
  destination: string | null;
  vehicle_type: string | null;
  availability: string | null;
  source: string;
  should_map: boolean;
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
let currentTelegramUser: TelegramUser | null = tg?.initDataUnsafe?.user || null;
let authToken: string | null = window.localStorage.getItem("rasylon_auth_token");
let selectedLocationId = "tashkent";
let radiusKm = 120;
let query = "";
let locations: LocationSignal[] = [];
let activities: DriverActivity[] = [];
let signalsError: string | null = null;
let yandexMapActive = false;

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
        <div id="realMap" class="real-map"></div>
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
          <span>Локаций на карте</span>
          <strong id="totalLocations">0</strong>
        </div>
      </section>

      <section class="section-head compact">
        <h1>Активность водителей</h1>
        <button data-screen-target="inbox">Сообщения</button>
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
        <span>Auto mailing</span>
        <strong>Рассылка запускается от выбранного номера по выбранным Telegram-группам.</strong>
      </section>
      <div class="section-head">
        <h1>Рассылка</h1>
        <button id="refreshMailingStatus">Статус</button>
      </div>
      <div class="mailing-status" id="mailingStatusBox">Проверяем доступ к рассылке...</div>
      <form class="panel mailing-panel" id="mailingForm">
        <label>Текст рассылки<textarea id="mailingMessage" placeholder="Например: Ташкент, тент, ищу груз на Алматы, готов сегодня"></textarea></label>
        <div class="otp-grid">
          <label>Интервал, минут<input id="mailingInterval" inputmode="numeric" value="10" /></label>
          <label>Номер<input id="mailingSender" readonly value="Проверяем..." /></label>
        </div>
        <div class="mailing-actions">
          <button class="secondary-button" type="button" id="selectAllGroups">Выбрать все группы</button>
          <button class="ghost-button" type="button" id="stopMailing">Остановить рассылку</button>
        </div>
        <button class="primary-button wide" type="submit">Запустить авторассылку</button>
        <div class="status" id="mailingStatus"></div>
      </form>
    </section>

    <section class="screen" data-screen="inbox">
      <section class="editorial-strip">
        <span>Messages</span>
        <strong>Сообщения от водителей, которые ищут груз, отсортированы по локации и направлению.</strong>
      </section>
      <div class="section-head">
        <h1>Сообщения</h1>
        <button id="refreshMessages">Обновить</button>
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
          <a class="telegram-login-link" id="manualBotLogin" href="#" target="_blank" rel="noopener noreferrer">Открыть Telegram для входа</a>
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
    <button class="nav-item" data-screen-target="messages">Рассылка</button>
    <button class="nav-item" data-screen-target="inbox">Сообщения</button>
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
    telegram_user: currentTelegramUser,
    auth_token: authToken,
  };
}

function setStatus(element: HTMLElement, message: string, ok = false): void {
  element.classList.toggle("ok", ok);
  element.textContent = message;
}

function setTelegramLoginLink(botUrl: string | null): void {
  const link = byId<HTMLAnchorElement>("manualBotLogin");
  if (!botUrl) {
    link.classList.remove("visible");
    link.href = "#";
    return;
  }
  link.href = botUrl;
  link.classList.add("visible");
}

function openTelegramLogin(botUrl: string | null): void {
  if (!botUrl) return;
  if (tg?.openTelegramLink) {
    tg.openTelegramLink(botUrl);
    return;
  }
  window.location.href = botUrl;
}

function setScreen(screen: string): void {
  const protectedScreens = ["dashboard", "locations", "messages", "inbox", "profile"];
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
    credentials: "include",
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "request_failed");
  return data as T;
}

function selectedLocation(): LocationSignal {
  return locations.find((location) => location.id === selectedLocationId) || locations[0] || {
    id: "tashkent",
    name: "Ташкент",
    country: "Узбекистан",
    lat: 41.31,
    lon: 69.28,
    drivers: 0,
    messages: 0,
    updated_at: "",
    trend: [],
    subscribed: false,
    favorite: false,
  };
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
  pins.innerHTML = yandexMapActive ? "" : renderFallbackMapPins();
  byId("radiusLabel").textContent = `${radiusKm} км`;
  byId("totalDrivers").textContent = String(locations.reduce((sum, location) => sum + location.drivers, 0));
  byId("totalMessages").textContent = String(locations.reduce((sum, location) => sum + location.messages, 0));
  byId("totalLocations").textContent = String(locations.length);
}

function renderFallbackMapPins(): string {
  return locations
    .map((location) => {
      const position = locationPosition(location);
      const active = location.id === selectedLocationId ? "active" : "";
      return `
        <button class="map-pin ${active}" data-location-id="${escapeHtml(location.id)}" style="left:${position.left}%;top:${position.top}%">
          <strong>${location.drivers}</strong>
          <span>${escapeHtml(location.name)}</span>
        </button>
      `;
    })
    .join("");
}

function markerElement(location: LocationSignal): HTMLElement {
  const button = document.createElement("button");
  button.type = "button";
  button.className = `geo-map-pin ${location.id === selectedLocationId ? "active" : ""}`;
  button.dataset.locationId = location.id;
  button.innerHTML = `
    <strong>${location.drivers}</strong>
    <span>${escapeHtml(location.name)}</span>
  `;
  return button;
}

function renderActivities(): void {
  const current = selectedLocation().name;
  const relevant = activities.filter((activity) => activity.location === current);
  const html = relevant
    .map((activity) => `
      <article class="activity-card">
        <div class="activity-head">
          <div>
            <strong>${escapeHtml(activity.driver)}</strong>
            <span>${escapeHtml(activity.username)} · ${activity.minutes_ago} мин назад</span>
          </div>
        </div>
        <p>${escapeHtml(activity.message)}</p>
        <div class="chips">
          <span>${escapeHtml(activity.location)}</span>
          <span>→ ${escapeHtml(activity.destination)}</span>
          <span>${escapeHtml(activity.vehicle_type)}</span>
          <span>${escapeHtml(activity.availability)}</span>
        </div>
        <footer>Источник: Telegram-группа @${escapeHtml(activity.source)}</footer>
      </article>
    `)
    .join("");
  byId("activityList").innerHTML = html || `<div class="empty">Пока нет реальных сигналов по этой локации.</div>`;
  const driverCargoRequests = activities
    .filter((activity) => activity.intent === "driver_searching_cargo")
    .sort((left, right) => left.minutes_ago - right.minutes_ago);
  if (signalsError) {
    byId("messageList").innerHTML = `<div class="empty">${escapeHtml(signalsError)}</div>`;
    return;
  }
  byId("messageList").innerHTML = driverCargoRequests.map((activity) => `
    <article class="activity-card source">
      <div class="activity-head"><strong>@${escapeHtml(activity.source)}</strong><span>${activity.minutes_ago} мин назад</span></div>
      <p>${escapeHtml(activity.message)}</p>
      <div class="chips"><span>${escapeHtml(activity.location)}</span><span>→ ${escapeHtml(activity.destination)}</span><span>${escapeHtml(activity.vehicle_type)}</span></div>
    </article>
  `).join("") || `<div class="empty">Пока нет реальных сообщений от водителей, которые ищут груз.</div>`;
}

function renderLocations(): void {
  byId<HTMLDivElement>("locationList").innerHTML = filteredLocations()
    .map((location) => `
      <article class="location-card ${location.id === selectedLocationId ? "active" : ""}" data-location-id="${escapeHtml(location.id)}">
        <div>
          <strong>${escapeHtml(location.name)}</strong>
          <span>${escapeHtml(location.country)} · обновлено ${escapeHtml(location.updated_at)}</span>
        </div>
        <div class="location-stats">
          <span>${location.drivers} водителей</span>
          <span>${location.messages} сообщений</span>
        </div>
        <div class="spark">${location.trend.map((value) => `<i style="height:${10 + value * 2}px"></i>`).join("")}</div>
        <div class="card-actions">
          <button data-toggle-sub="${escapeHtml(location.id)}">${location.subscribed ? "Отписаться" : "Подписаться"}</button>
          <button data-toggle-fav="${escapeHtml(location.id)}">${location.favorite ? "В избранном" : "В избранное"}</button>
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
  await initRealMap();
}

function loadScript(src: string): Promise<void> {
  return new Promise((resolve, reject) => {
    if (document.querySelector(`script[src="${src}"]`)) {
      resolve();
      return;
    }
    const script = document.createElement("script");
    script.src = src;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error("script_load_failed"));
    document.head.appendChild(script);
  });
}

async function initRealMap(): Promise<void> {
  const container = byId<HTMLDivElement>("realMap");
  const selected = selectedLocation();
  const apiKey = config?.maps?.yandex_api_key || import.meta.env.VITE_YANDEX_MAPS_API_KEY || "";
  if (apiKey) {
    try {
      await loadScript(`https://api-maps.yandex.ru/v3/?apikey=${encodeURIComponent(apiKey)}&lang=ru_RU`);
      await window.ymaps3.ready;
      const { YMap, YMapDefaultSchemeLayer, YMapDefaultFeaturesLayer, YMapMarker } = window.ymaps3;
      container.innerHTML = "";
      yandexMapActive = true;
      byId<HTMLDivElement>("mapPins").innerHTML = "";
      const map = new YMap(container, {
        location: { center: [selected.lon, selected.lat], zoom: 6 },
      });
      map.addChild(new YMapDefaultSchemeLayer());
      map.addChild(new YMapDefaultFeaturesLayer());
      locations.forEach((location) => {
        map.addChild(
          new YMapMarker(
            { coordinates: [location.lon, location.lat] },
            markerElement(location),
          ),
        );
      });
      return;
    } catch {
      yandexMapActive = false;
      container.innerHTML = "";
    }
  }
  yandexMapActive = false;
  const bbox = "65.5,38.2,78.8,44.6";
  container.innerHTML = `<iframe title="OpenStreetMap" src="https://www.openstreetmap.org/export/embed.html?bbox=${bbox}&layer=mapnik&marker=${selected.lat},${selected.lon}" loading="lazy"></iframe>`;
  byId<HTMLDivElement>("mapPins").innerHTML = renderFallbackMapPins();
}

async function loadSignals(): Promise<void> {
  if (!isAuthenticated) {
    signalsError = null;
    renderAll();
    return;
  }
  try {
    const data = await postJson<{ locations?: LocationSignal[]; activities?: DriverActivity[]; scope?: string }>("/api/mini/signals", {
      ...basePayload(),
      limit: 100,
    });
    signalsError = null;
    locations = Array.isArray(data.locations) ? data.locations : [];
    activities = Array.isArray(data.activities) ? data.activities : [];
    if (locations.length && !locations.some((location) => location.id === selectedLocationId)) {
      selectedLocationId = locations[0].id;
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "";
    if (message === "auth_required") {
      isAuthenticated = false;
      authToken = null;
      window.localStorage.removeItem("rasylon_auth_token");
      signalsError = "Сессия входа истекла. Войдите через Telegram еще раз.";
      setScreen("login");
    } else {
      signalsError = "Не удалось загрузить реальные сообщения. Попробуйте обновить.";
    }
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

function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function reasonText(reason: string): string {
  return {
    no_targets: "Не выбраны Telegram-группы для рассылки.",
    payment_required: "Нет активной оплаты пользователя.",
    system_payment_required: "Общая оплата сервиса не активна.",
  }[reason] || reason;
}

async function loadMailingStatus(): Promise<void> {
  const box = byId("mailingStatusBox");
  try {
    const data = await postJson<{
      is_enabled: boolean;
      target_count: number;
      can_start: boolean;
      reasons: string[];
      sender_account?: { title?: string; phone?: string; username?: string } | null;
    }>("/api/mini/mailing/status", basePayload());
    const account = data.sender_account?.title || data.sender_account?.phone || data.sender_account?.username || "бот";
    byId<HTMLInputElement>("mailingSender").value = account;
    box.innerHTML = `
      <strong>${data.can_start ? "Готово к запуску" : "Нужно настроить"}</strong>
      <span>Групп выбрано: ${data.target_count}</span>
      <span>Отправитель: ${escapeHtml(account)}</span>
      <span>Статус: ${data.is_enabled ? "рассылка активна" : "рассылка остановлена"}</span>
      ${data.reasons.length ? `<em>${escapeHtml(data.reasons.map(reasonText).join(" "))}</em>` : ""}
    `;
  } catch {
    byId<HTMLInputElement>("mailingSender").value = "Требуется вход";
    box.textContent = "Войдите через Telegram, чтобы увидеть статус рассылки.";
  }
}

async function classifyMailingMessage(): Promise<MessageClassification | null> {
  const message = byId<HTMLTextAreaElement>("mailingMessage").value.trim();
  if (!message) {
    return null;
  }
  const data = await postJson<{ classification: MessageClassification }>("/api/ai/classify-message", { message });
  return data.classification;
}

async function startMailing(event: SubmitEvent): Promise<void> {
  event.preventDefault();
  const status = byId<HTMLDivElement>("mailingStatus");
  setStatus(status, "");
  try {
    const classification = await classifyMailingMessage();
    const message = byId<HTMLTextAreaElement>("mailingMessage").value.trim();
    const interval = Number(byId<HTMLInputElement>("mailingInterval").value || "10");
    const result = await postJson<{ started?: boolean; reasons?: string[] }>("/api/mini/mailing/start", {
      ...basePayload(),
      message,
      interval_minutes: interval,
      classification,
    });
    if (!result.started) {
      setStatus(status, (result.reasons || []).map(reasonText).join(" ") || "Рассылка не запущена.");
      await loadMailingStatus();
      return;
    }
    setStatus(status, "Авторассылка запущена. Бот будет отправлять сообщение по расписанию.", true);
    await loadMailingStatus();
  } catch (error) {
    const code = error instanceof Error ? error.message : "";
    setStatus(
      status,
      code === "auth_required"
        ? "Сначала войдите через Telegram."
        : code === "message_required"
          ? "Введите текст рассылки."
          : "Не удалось запустить рассылку.",
    );
  }
}

async function selectAllGroups(): Promise<void> {
  const status = byId<HTMLDivElement>("mailingStatus");
  setStatus(status, "");
  try {
    const result = await postJson<{ selected_count?: number }>("/api/mini/mailing/select-all", basePayload());
    setStatus(status, `Выбрано групп: ${result.selected_count || 0}.`, true);
    await loadMailingStatus();
  } catch (error) {
    const code = error instanceof Error ? error.message : "";
    setStatus(status, code === "auth_required" ? "Сначала войдите через Telegram." : "Не удалось выбрать группы.");
  }
}

async function stopMailing(): Promise<void> {
  const status = byId<HTMLDivElement>("mailingStatus");
  setStatus(status, "");
  try {
    await postJson("/api/mini/mailing/stop", basePayload());
    setStatus(status, "Авторассылка остановлена.", true);
    await loadMailingStatus();
  } catch (error) {
    const code = error instanceof Error ? error.message : "";
    setStatus(status, code === "auth_required" ? "Сначала войдите через Telegram." : "Не удалось остановить рассылку.");
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
  setTelegramLoginLink(null);
  try {
    const response = await postJson<BrowserLoginStartResponse>("/api/auth/browser-login/start", {});
    browserLoginToken = response.token;
    if (browserLoginPoll !== null) window.clearInterval(browserLoginPoll);
    browserLoginPoll = window.setInterval(() => void checkBrowserLogin(), 1800);
    setTelegramLoginLink(response.bot_url);
    setStatus(status, "Откройте Telegram по кнопке выше и нажмите Start. После подтверждения dashboard откроется автоматически.", true);
    openTelegramLogin(response.bot_url);
  } catch {
    setStatus(status, "Не удалось создать ссылку для входа. Попробуйте позже.");
  }
}

async function checkBrowserLogin(): Promise<void> {
  if (!browserLoginToken) return;
  try {
    const response = await fetch(`${API_BASE}/api/auth/browser-login/check?token=${encodeURIComponent(browserLoginToken)}`, {
      credentials: "include",
    });
    const data = await response.json() as { status?: string; telegram_user?: TelegramUser; auth_token?: string };
    if (data.status === "confirmed") {
      if (browserLoginPoll !== null) window.clearInterval(browserLoginPoll);
      browserLoginPoll = null;
      browserLoginToken = null;
      isAuthenticated = true;
      if (data.auth_token) {
        authToken = data.auth_token;
        window.localStorage.setItem("rasylon_auth_token", data.auth_token);
      }
      const userInfo = data.telegram_user;
      if (userInfo) {
        currentTelegramUser = userInfo;
        const name = [userInfo.first_name, userInfo.last_name].filter(Boolean).join(" ") || userInfo.username || "Telegram user";
        byId("profileName").textContent = name;
        byId("profileMeta").textContent = userInfo.username ? `@${userInfo.username}` : `ID ${userInfo.id}`;
        byId("telegramValue").textContent = userInfo.username ? `@${userInfo.username}` : `ID ${userInfo.id}`;
      }
      setScreen("dashboard");
      await loadSignals();
      await loadMailingStatus();
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
    await loadSignals();
  } catch (error) {
    const code = error instanceof Error ? error.message : "";
    setStatus(status, code === "otp_expired" ? "OTP истек. Запросите новый код." : code === "too_many_attempts" ? "Слишком много попыток. Попробуйте позже." : "Неверный OTP.");
  }
}

function renderAll(): void {
  renderMap();
  renderActivities();
  renderLocations();
  void initRealMap();
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
byId<HTMLButtonElement>("refreshMessages").addEventListener("click", () => void loadSignals());
byId<HTMLButtonElement>("refreshMailingStatus").addEventListener("click", () => void loadMailingStatus());
byId<HTMLButtonElement>("selectAllGroups").addEventListener("click", () => void selectAllGroups());
byId<HTMLButtonElement>("stopMailing").addEventListener("click", () => void stopMailing());
byId<HTMLFormElement>("mailingForm").addEventListener("submit", (event) => void startMailing(event as SubmitEvent));
byId<HTMLButtonElement>("openBotLogin").addEventListener("click", () => void startBrowserLogin());
byId<HTMLButtonElement>("sendOtp").addEventListener("click", () => void sendOtp());
byId<HTMLFormElement>("otpForm").addEventListener("submit", (event) => void verifyOtp(event as SubmitEvent));
byId<HTMLFormElement>("paymentForm").addEventListener("submit", (event) => void submitPayment(event as SubmitEvent));
byId<HTMLButtonElement>("openBot").addEventListener("click", () => {
  const url = config?.bot.url || "https://t.me/atRasylon_bot";
  tg ? tg.openTelegramLink(url) : window.location.assign(url);
});
byId<HTMLButtonElement>("logoutButton").addEventListener("click", () => {
  void postJson("/api/auth/logout", basePayload()).catch(() => undefined);
  isAuthenticated = false;
  authToken = null;
  window.localStorage.removeItem("rasylon_auth_token");
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
  isAuthenticated = true;
  const name = [user.first_name, user.last_name].filter(Boolean).join(" ") || user.username || "Telegram user";
  byId("profileName").textContent = name;
  byId("profileMeta").textContent = user.username ? `@${user.username}` : "Telegram WebApp";
  byId("telegramValue").textContent = user.username ? `@${user.username}` : `ID ${user.id}`;
}
if (authToken) {
  isAuthenticated = true;
  activeScreen = "dashboard";
}

renderAll();
void loadConfig();
void loadSignals();
void loadMailingStatus();
setScreen(activeScreen);
