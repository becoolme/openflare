// @ts-check
import { defineConfig } from 'astro/config';

// Fully static build. `dist/` is uploaded as Workers static assets
// (see wrangler.jsonc); there is no server-side code.
export default defineConfig({
  output: 'static',
  build: { format: 'file' },
});
