export default {
  fetch() {
    return new Response("ok");
  },
};

// Exported and named like the declaration — but a PLAIN class, not a
// WorkflowEntrypoint subclass. `vrt deploy` must reject this pack.
export class OrderFlow {
  async run() {
    return "never-runs";
  }
}
