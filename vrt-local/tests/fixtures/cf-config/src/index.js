// Referenced with `with { type: "cf-worker" }`: the loader must synthesize
// the specifier and never import or execute this module. Throwing at the top
// level makes any accidental evaluation fail loudly.
throw new Error("cf-worker entrypoint must not be executed by the config loader");

export default {
  fetch() {
    return new Response("cf-config fixture");
  },
};
