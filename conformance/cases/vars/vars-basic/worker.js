export default {
  async fetch(_request, env) {
    return Response.json({ value: env.CONFORMANCE_VALUE });
  },
};
