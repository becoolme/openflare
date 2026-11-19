export default {
  async fetch(request, env) {
    const greeting = env.GREETING ?? "(var missing)";
    // Secret presence only — a real worker would use the value; the
    // demo reports whether the binding resolved.
    const secretPresent = env.GREETING_SECRET !== undefined;
    return Response.json({ greeting, secretPresent });
  },
};
