export default {
  async fetch(request, env) {
    return Response.json({
      greeting: env.GREETING ?? "(var missing)",
      stage: env.STAGE ?? "(no stage)",
      preview: env.PREVIEW,
    });
  },
};
