export default {
  async fetch(request) {
    return new Response(`worker:${new URL(request.url).pathname}`);
  },
};
