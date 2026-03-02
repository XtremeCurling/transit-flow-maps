import { defineConfig } from "vite";

export default defineConfig({
  base: "/transit-flow-maps/",
  root: "src",
  build: {
    outDir: "../../docs",
    emptyOutDir: true
  }
});
