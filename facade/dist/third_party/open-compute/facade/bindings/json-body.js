export function bindingBody(input, maximum, errorCode) {
    let size = 0;
    return input?.pipeThrough(new TransformStream({
        transform (chunk, controller) {
            size += chunk.byteLength;
            if (size > maximum) throw new TypeError(errorCode);
            controller.enqueue(chunk);
        }
    }));
}
export async function bindingJson(request, maximum, errorCode) {
    const body = bindingBody(request.body, maximum, errorCode);
    try {
        return await new Response(body).json();
    } catch (error) {
        if (error instanceof SyntaxError) throw new TypeError(errorCode);
        throw error;
    }
}
