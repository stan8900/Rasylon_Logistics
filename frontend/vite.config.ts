import { defineConfig } from "vite";

export default defineConfig({
  preview: {
    allowedHosts: [
      "rasylonlogisticsfrontend-production.up.railway.app",
      "rasylon.uz",
      "www.rasylon.uz",
    ],
  },
});
