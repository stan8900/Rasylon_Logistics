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
  close: () => void;
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

type Vehicle = {
  name: string;
  meta: string;
  category: "freight" | "special" | "light";
  color: "orange" | "blue" | "dark" | "green";
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

const vehicles: Vehicle[] = [
  { name: "Тент", meta: "до 22 т", category: "freight", color: "orange" },
  { name: "Рефрижератор", meta: "температура", category: "freight", color: "blue" },
  { name: "Изотерм", meta: "продукты", category: "freight", color: "dark" },
  { name: "Фура", meta: "86-110 м3", category: "freight", color: "orange" },
  { name: "Контейнер", meta: "20/40 ft", category: "freight", color: "blue" },
  { name: "Бортовой", meta: "открытый", category: "freight", color: "dark" },
  { name: "Самосвал", meta: "сыпучие", category: "special", color: "green" },
  { name: "Трал", meta: "негабарит", category: "special", color: "dark" },
  { name: "Эвакуатор", meta: "авто", category: "special", color: "blue" },
  { name: "Манипулятор", meta: "погрузка", category: "special", color: "green" },
  { name: "Газель", meta: "город", category: "light", color: "blue" },
  { name: "Лабо", meta: "малый груз", category: "light", color: "orange" }
];

const categories = [
  { key: "all", label: "Все" },
  { key: "freight", label: "Грузовые" },
  { key: "special", label: "Спецтехника" },
  { key: "light", label: "Малотоннаж" }
] as const;

let activeScreen = "home";
let activeCategory = "all";
let activeVehicle = vehicles[0].name;
let config: MiniConfig | null = null;

const app = document.querySelector<HTMLDivElement>("#app");
if (!app) throw new Error("App root not found");

app.innerHTML = `
  <main class="app-shell">
    <header class="topbar">
      <button class="icon-button" data-action="home" aria-label="Главная">
        <span class="chevron">‹</span>
      </button>
      <div class="brand">
        Rasylon Logistics
        <span>грузы, техника, рассылки</span>
      </div>
      <button class="icon-button" data-action="support" aria-label="Поддержка">?</button>
    </header>

    <section class="screen active" data-screen="home">
      <div class="map-panel">
        <div class="road road-a"></div>
        <div class="road road-b"></div>
        <div class="route-line"></div>
        <div class="pin pin-from"></div>
        <div class="pin pin-to"></div>
        <div class="truck-badge">
          <div class="mini-truck"></div>
        </div>
        <div class="map-summary">
          <div>
            <strong>Найдите машину или груз быстрее</strong>
            <span>Заявка уходит админам и в Telegram-бот</span>
          </div>
          <button class="primary-button" data-screen-target="order">Создать</button>
        </div>
      </div>

      <div class="search-row">
        <input id="searchInput" placeholder="Поиск: тент, реф, самосвал" />
        <button class="icon-button" id="clearSearch" aria-label="Очистить">×</button>
      </div>

      <div class="tabs" id="categoryTabs"></div>

      <div class="section-head">
        <h1>Грузовой транспорт</h1>
        <button data-screen-target="order">Заявка</button>
      </div>
      <div class="vehicle-grid" id="vehicleGrid"></div>

      <div class="metrics">
        <div><strong>22</strong><span>чата для рассылки</span></div>
        <div><strong>10</strong><span>каналов подключено</span></div>
        <div><strong>24/7</strong><span>заявки админам</span></div>
      </div>
    </section>

    <section class="screen" data-screen="order">
      <form class="panel" id="orderForm">
        <h1>Создать заявку</h1>
        <p>Укажите маршрут и технику. Админ получит заявку в Telegram и сможет быстро связаться.</p>
        <div class="fields">
          <label>Откуда<input id="fromInput" autocomplete="address-level2" placeholder="Ташкент" /></label>
          <label>Куда<input id="toInput" autocomplete="address-level2" placeholder="Москва" /></label>
          <label>Тип транспорта<select id="truckInput"></select></label>
          <label>Вес / объём<input id="weightInput" placeholder="20 т / 86 м3" /></label>
          <label>Дата<input id="dateInput" type="date" /></label>
          <label>Телефон<input id="phoneInput" inputmode="tel" autocomplete="tel" placeholder="+998..." /></label>
          <label class="full">Комментарий<textarea id="noteInput" placeholder="Например: нужен тент, загрузка сегодня, оплата нал"></textarea></label>
        </div>
        <button class="primary-button wide" type="submit">Отправить заявку</button>
        <div class="status" id="orderStatus"></div>
      </form>
    </section>

    <section class="screen" data-screen="pay">
      <form class="panel" id="paymentForm">
        <h1>Оплата доступа</h1>
        <p>После оплаты отправьте заявку. Админ подтвердит доступ к авторассылке.</p>
        <div class="pay-grid">
          <div><span>Сумма</span><strong id="amountText">...</strong></div>
          <div><span>Карта</span><strong id="cardText">...</strong></div>
        </div>
        <div class="fields">
          <label>Telegram номер<input id="telegramPhone" inputmode="tel" autocomplete="tel" placeholder="+998..." /></label>
          <label>WhatsApp номер<input id="whatsappPhone" inputmode="tel" autocomplete="tel" placeholder="+998..." /></label>
          <label>Карта, с которой оплатили<input id="cardNumber" inputmode="numeric" autocomplete="cc-number" placeholder="8600 ...." /></label>
          <label>Имя на карте<input id="cardName" autocomplete="cc-name" placeholder="Имя Фамилия" /></label>
        </div>
        <button class="primary-button wide" type="submit">Отправить на проверку</button>
        <div class="status" id="payStatus"></div>
      </form>
    </section>

    <section class="screen" data-screen="profile">
      <div class="panel">
        <h1>Профиль</h1>
        <p>Быстрые действия для Telegram-бота и поддержки.</p>
        <div class="profile-actions">
          <button class="primary-button wide" id="openBot">Открыть бота</button>
          <button class="secondary-button wide" id="openSupport">Поддержка</button>
        </div>
      </div>
    </section>
  </main>

  <nav class="bottom-nav">
    <button class="nav-item active" data-screen-target="home">Главная</button>
    <button class="nav-item" data-screen-target="order">Заявка</button>
    <button class="nav-item" data-screen-target="pay">Оплата</button>
    <button class="nav-item" data-screen-target="profile">Профиль</button>
  </nav>
`;

function byId<T extends HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) throw new Error(`Missing element: ${id}`);
  return element as T;
}

function setScreen(screen: string): void {
  activeScreen = screen;
  document.querySelectorAll<HTMLElement>(".screen").forEach((element) => {
    element.classList.toggle("active", element.dataset.screen === screen);
  });
  document.querySelectorAll<HTMLButtonElement>(".nav-item").forEach((button) => {
    button.classList.toggle("active", button.dataset.screenTarget === screen);
  });
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function renderCategories(): void {
  const tabs = byId<HTMLDivElement>("categoryTabs");
  tabs.innerHTML = categories
    .map((category) => `<button class="tab ${activeCategory === category.key ? "active" : ""}" data-category="${category.key}">${category.label}</button>`)
    .join("");
}

function renderVehicles(): void {
  const query = byId<HTMLInputElement>("searchInput").value.trim().toLowerCase();
  const filtered = vehicles.filter((vehicle) => {
    const categoryMatches = activeCategory === "all" || vehicle.category === activeCategory;
    const queryMatches = !query || vehicle.name.toLowerCase().includes(query);
    return categoryMatches && queryMatches;
  });
  byId<HTMLDivElement>("vehicleGrid").innerHTML = filtered
    .map((vehicle) => `
      <button class="vehicle-card ${activeVehicle === vehicle.name ? "active" : ""}" data-vehicle="${vehicle.name}">
        <span class="truck ${vehicle.color}"></span>
        <strong>${vehicle.name}</strong>
        <span>${vehicle.meta}</span>
      </button>
    `)
    .join("");
  byId<HTMLSelectElement>("truckInput").innerHTML = vehicles
    .map((vehicle) => `<option ${activeVehicle === vehicle.name ? "selected" : ""}>${vehicle.name}</option>`)
    .join("");
}

function basePayload(): Record<string, unknown> {
  return {
    tg_init_data: tg?.initData || "",
    telegram_user: tg?.initDataUnsafe?.user || null
  };
}

async function postJson<T>(path: string, payload: Record<string, unknown>): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "request_failed");
  return data as T;
}

function setStatus(element: HTMLElement, message: string, ok = false): void {
  element.classList.toggle("ok", ok);
  element.textContent = message;
}

async function loadConfig(): Promise<void> {
  const response = await fetch(`${API_BASE}/api/mini/config`);
  config = await response.json() as MiniConfig;
  byId("amountText").textContent = config.payment.amount_text;
  byId("cardText").textContent = config.payment.card_target;
}

async function submitOrder(event: SubmitEvent): Promise<void> {
  event.preventDefault();
  const status = byId<HTMLDivElement>("orderStatus");
  setStatus(status, "");
  try {
    await postJson("/api/mini/order", {
      ...basePayload(),
      from: byId<HTMLInputElement>("fromInput").value,
      to: byId<HTMLInputElement>("toInput").value,
      truck_type: byId<HTMLSelectElement>("truckInput").value,
      weight: byId<HTMLInputElement>("weightInput").value,
      date: byId<HTMLInputElement>("dateInput").value,
      phone: byId<HTMLInputElement>("phoneInput").value,
      note: byId<HTMLTextAreaElement>("noteInput").value
    });
    tg?.HapticFeedback?.notificationOccurred("success");
    setStatus(status, "Заявка отправлена. Админ свяжется с вами.", true);
  } catch (error) {
    tg?.HapticFeedback?.notificationOccurred("error");
    const code = error instanceof Error ? error.message : "";
    setStatus(status, code === "phone_required" ? "Укажите телефон." : code === "route_required" ? "Укажите маршрут и транспорт." : "Не удалось отправить заявку.");
  }
}

async function submitPayment(event: SubmitEvent): Promise<void> {
  event.preventDefault();
  const status = byId<HTMLDivElement>("payStatus");
  setStatus(status, "");
  try {
    await postJson("/api/mini/payment", {
      ...basePayload(),
      telegram_phone: byId<HTMLInputElement>("telegramPhone").value,
      whatsapp_phone: byId<HTMLInputElement>("whatsappPhone").value,
      card_number: byId<HTMLInputElement>("cardNumber").value,
      card_name: byId<HTMLInputElement>("cardName").value
    });
    tg?.HapticFeedback?.notificationOccurred("success");
    setStatus(status, "Заявка на оплату отправлена админам.", true);
  } catch (error) {
    tg?.HapticFeedback?.notificationOccurred("error");
    const code = error instanceof Error ? error.message : "";
    setStatus(status, code === "phone_required" ? "Укажите Telegram или WhatsApp номер." : code === "bad_card" ? "Проверьте номер карты." : code === "bad_name" ? "Укажите имя на карте." : "Не удалось отправить заявку.");
  }
}

document.addEventListener("click", (event) => {
  const target = event.target as HTMLElement;
  const screenButton = target.closest<HTMLElement>("[data-screen-target]");
  if (screenButton?.dataset.screenTarget) {
    setScreen(screenButton.dataset.screenTarget);
    return;
  }
  const categoryButton = target.closest<HTMLElement>("[data-category]");
  if (categoryButton?.dataset.category) {
    activeCategory = categoryButton.dataset.category;
    renderCategories();
    renderVehicles();
    return;
  }
  const vehicleButton = target.closest<HTMLElement>("[data-vehicle]");
  if (vehicleButton?.dataset.vehicle) {
    activeVehicle = vehicleButton.dataset.vehicle;
    byId<HTMLInputElement>("searchInput").value = "";
    renderVehicles();
    setScreen("order");
  }
});

byId<HTMLInputElement>("searchInput").addEventListener("input", renderVehicles);
byId<HTMLButtonElement>("clearSearch").addEventListener("click", () => {
  byId<HTMLInputElement>("searchInput").value = "";
  renderVehicles();
});
byId<HTMLFormElement>("orderForm").addEventListener("submit", (event) => void submitOrder(event as SubmitEvent));
byId<HTMLFormElement>("paymentForm").addEventListener("submit", (event) => void submitPayment(event as SubmitEvent));
byId<HTMLButtonElement>("openBot").addEventListener("click", () => {
  const url = config?.bot.url || "https://t.me/atRasylon_bot";
  tg ? tg.openTelegramLink(url) : window.location.assign(url);
});
byId<HTMLButtonElement>("openSupport").addEventListener("click", () => {
  const support = (config?.bot.support || "@rasylon_support").replace("@", "");
  const url = `https://t.me/${support}`;
  tg ? tg.openTelegramLink(url) : window.location.assign(url);
});
document.querySelector<HTMLElement>('[data-action="home"]')?.addEventListener("click", () => setScreen("home"));
document.querySelector<HTMLElement>('[data-action="support"]')?.addEventListener("click", () => byId<HTMLButtonElement>("openSupport").click());

renderCategories();
renderVehicles();
void loadConfig();
setScreen(activeScreen);
