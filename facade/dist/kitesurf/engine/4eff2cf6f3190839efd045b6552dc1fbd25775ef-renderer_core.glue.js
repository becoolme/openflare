/* @ts-self-types="./renderer_core.d.ts" */

/**
 * JPEG encoder that consumes the recorded scene in MCU-aligned bands.
 */
export class SceneJpegStream {
    static __wrap(ptr) {
        const obj = Object.create(SceneJpegStream.prototype);
        obj.__wbg_ptr = ptr;
        SceneJpegStreamFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        SceneJpegStreamFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        __wbg_call_guard();
        try {
            wasm.__wbg_scenejpegstream_free(ptr, 0);
        } catch(e) {
            __wbg_handle_catch(e);
        }

    }
    /**
     * Encode the next band, returning empty bytes only after the final chunk.
     * @returns {Uint8Array}
     */
    nextChunk() {
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenejpegstream_nextChunk(this.__wbg_ptr);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        if (ret[3]) {
            throw takeFromExternrefTable0(ret[2]);
        }
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
}
if (Symbol.dispose) SceneJpegStream.prototype[Symbol.dispose] = SceneJpegStream.prototype.free;

/**
 * Wasm replay interface with one session resource cache per instance.
 * Raster and PDF failures return empty bytes; diagnostics are available via `lastRasterizeInfo`.
 */
export class SceneRasterizer {
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        SceneRasterizerFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        __wbg_call_guard();
        try {
            wasm.__wbg_scenerasterizer_free(ptr, 0);
        } catch(e) {
            __wbg_handle_catch(e);
        }

    }
    /**
     * Retire an engine-store image key whose payload is no longer available.
     * Later missing-resource scans skip it; absent images paint as a no-op.
     * @param {string} key
     * @returns {number}
     */
    abandonImage(key) {
        const ptr0 = passStringToWasm0(key, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_abandonImage(this.__wbg_ptr, ptr0, len0);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        return ret >>> 0;
    }
    /**
     * @param {StreamingPdf} pdf
     * @param {Uint8Array} scene
     * @returns {boolean}
     */
    appendPdfPage(pdf, scene) {
        _assertClass(pdf, StreamingPdf);
        const ptr0 = passArray8ToWasm0(scene, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_appendPdfPage(this.__wbg_ptr, pdf.__wbg_ptr, ptr0, len0);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        return ret !== 0;
    }
    /**
     * @param {number} paper_width
     * @param {number} paper_height
     * @param {boolean} omit_background
     * @param {number} margin_top
     * @param {number} margin_right
     * @param {number} margin_bottom
     * @param {number} margin_left
     * @returns {StreamingPdf}
     */
    beginPdf(paper_width, paper_height, omit_background, margin_top, margin_right, margin_bottom, margin_left) {
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_beginPdf(this.__wbg_ptr, paper_width, paper_height, omit_background, margin_top, margin_right, margin_bottom, margin_left);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        return StreamingPdf.__wrap(ret);
    }
    /**
     * Apply `scene` at viewport-space `(dst_x, dst_y)` without encoding.
     * Returns `false` if `key` does not match the base or the scene cannot be replayed.
     * @param {Uint8Array} scene
     * @param {number} dst_x
     * @param {number} dst_y
     * @param {string} key
     * @returns {boolean}
     */
    blitPartial(scene, dst_x, dst_y, key) {
        const ptr0 = passArray8ToWasm0(scene, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(key, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_blitPartial(this.__wbg_ptr, ptr0, len0, dst_x, dst_y, ptr1, len1);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        return ret !== 0;
    }
    /**
     * Encode the retained frame, re-encoding only rows changed since its
     * last encode.
     * Returns empty bytes when `key` is not the retained base or encoding fails.
     * @param {string} format
     * @param {number} quality
     * @param {string} key
     * @returns {Uint8Array}
     */
    encodeRetained(format, quality, key) {
        const ptr0 = passStringToWasm0(format, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(key, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_encodeRetained(this.__wbg_ptr, ptr0, len0, quality, ptr1, len1);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        var v3 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v3;
    }
    /**
     * @param {StreamingPdf} pdf
     * @returns {Uint8Array}
     */
    finishPdf(pdf) {
        _assertClass(pdf, StreamingPdf);
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_finishPdf(this.__wbg_ptr, pdf.__wbg_ptr);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
    /**
     * Diagnostics for the last rasterize call: JSON with the
     * error string (empty = ok) + degraded-frame counters.
     * @returns {string}
     */
    lastRasterizeInfo() {
        let deferred1_0;
        let deferred1_1;
        try {
            let ret;
            __wbg_call_guard();
            try {
                ret = wasm.scenerasterizer_lastRasterizeInfo(this.__wbg_ptr);
            } catch(e) {
                __wbg_handle_catch(e);
            }
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            __wbg_call_guard();
            try {
                wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
            } catch(e) {
                __wbg_handle_catch(e);
            }
        }
    }
    /**
     * Scan a framed scene for resources the store can't satisfy.
     * Returns [count u32] + count × { kind u8, hash u64 (LE) }.
     * @param {Uint8Array} scene
     * @returns {Uint8Array}
     */
    missingResources(scene) {
        const ptr0 = passArray8ToWasm0(scene, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_missingResources(this.__wbg_ptr, ptr0, len0);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        var v2 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v2;
    }
    /**
     * Union missingResources over every scene in a PDF envelope
     * ([count u32] then per scene [len u32][framed bytes] — see
     * DomHandle::recordPdfScenes). Same wire encoding as
     * missingResources so the manager's split/fetch loop is shared.
     * @param {Uint8Array} envelope
     * @returns {Uint8Array}
     */
    missingResourcesPdf(envelope) {
        const ptr0 = passArray8ToWasm0(envelope, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_missingResourcesPdf(this.__wbg_ptr, ptr0, len0);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        var v2 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v2;
    }
    /**
     * Create a rasterizer with an empty resource store and no retained frame.
     */
    constructor() {
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_new();
        } catch(e) {
            __wbg_handle_catch(e);
        }
        this.__wbg_ptr = ret;
        SceneRasterizerFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * JSON array of { hash: "<u64 as decimal string>", path }
     * entries for font asset-refs awaiting putFontAsset.
     * @returns {string}
     */
    pendingFontAssets() {
        let deferred1_0;
        let deferred1_1;
        try {
            let ret;
            __wbg_call_guard();
            try {
                ret = wasm.scenerasterizer_pendingFontAssets(this.__wbg_ptr);
            } catch(e) {
                __wbg_handle_catch(e);
            }
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            __wbg_call_guard();
            try {
                wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
            } catch(e) {
                __wbg_handle_catch(e);
            }
        }
    }
    /**
     * Engine-store image keys awaiting putImageBytes, as decimal
     * strings (a u64 doesn't survive a JS number). Format-3 refs
     * land here instead of being stored — see IMAGE_FORMAT_ENGINE_REF.
     * @returns {string[]}
     */
    pendingImages() {
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_pendingImages(this.__wbg_ptr);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        var v1 = getArrayJsValueFromWasm0(ret[0], ret[1]);
        wasm.__wbindgen_free(ret[0], ret[1] * 4, 4);
        return v1;
    }
    /**
     * Register a fetched font asset file (post-fetch bytes; woff2
     * is decompressed here). `path` matches pending asset-refs —
     * see ResourceStore::put_font_asset for why path, not hash.
     * @param {string} path
     * @param {Uint8Array} bytes
     * @returns {number}
     */
    putFontAsset(path, bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_putFontAsset(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        return ret >>> 0;
    }
    /**
     * Supply the payload for one engine-store image ref. Returns 1 on
     * a successful decode, 0 for an unknown key or undecodable bytes.
     * @param {string} key
     * @param {Uint8Array} bytes
     * @returns {number}
     */
    putImageBytes(key, bytes) {
        const ptr0 = passStringToWasm0(key, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_putImageBytes(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        return ret >>> 0;
    }
    /**
     * Ingest a PageScript render_rbi_resources reply. Returns the
     * number of entries stored; asset-refs land in the pending
     * list instead (see pendingFontAssets).
     * @param {Uint8Array} batch
     * @returns {number}
     */
    putResources(batch) {
        const ptr0 = passArray8ToWasm0(batch, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_putResources(this.__wbg_ptr, ptr0, len0);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        return ret >>> 0;
    }
    /**
     * Rasterize a framed scene to an encoded image. Empty Vec on
     * failure — read lastError for the reason. Format string and
     * quality clamp semantics match screenshot.rs encoders.
     * @param {Uint8Array} scene
     * @param {string} format
     * @param {number} quality
     * @param {boolean} omit_background
     * @returns {Uint8Array}
     */
    rasterize(scene, format, quality, omit_background) {
        const ptr0 = passArray8ToWasm0(scene, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(format, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_rasterize(this.__wbg_ptr, ptr0, len0, ptr1, len1, quality, omit_background);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        var v3 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v3;
    }
    /**
     * Rasterize a full frame and retain its RGBA under `key` for later partial updates.
     * Only one base is kept per instance; oversized frames are returned but not retained.
     * @param {Uint8Array} scene
     * @param {string} format
     * @param {number} quality
     * @param {boolean} omit_background
     * @param {string} key
     * @param {number} encoded_limit
     * @returns {Uint8Array}
     */
    rasterizeAndRetain(scene, format, quality, omit_background, key, encoded_limit) {
        const ptr0 = passArray8ToWasm0(scene, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(format, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(key, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_rasterizeAndRetain(this.__wbg_ptr, ptr0, len0, ptr1, len1, quality, omit_background, ptr2, len2, encoded_limit);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        var v4 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v4;
    }
    /**
     * Rasterize PNG or JPEG with a limit on encoded output bytes.
     * @param {Uint8Array} scene
     * @param {string} format
     * @param {number} quality
     * @param {boolean} omit_background
     * @param {number} limit
     * @returns {Uint8Array}
     */
    rasterizeLimited(scene, format, quality, omit_background, limit) {
        const ptr0 = passArray8ToWasm0(scene, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(format, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_rasterizeLimited(this.__wbg_ptr, ptr0, len0, ptr1, len1, quality, omit_background, limit);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        var v3 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v3;
    }
    /**
     * Composite a damage-rect scene at viewport-space `(dst_x, dst_y)` and encode the full frame.
     * Requires a retained frame with the same `key`.
     * Failures return empty bytes and set `lastRasterizeInfo`.
     * @param {Uint8Array} scene
     * @param {number} dst_x
     * @param {number} dst_y
     * @param {string} format
     * @param {number} quality
     * @param {string} key
     * @returns {Uint8Array}
     */
    rasterizePartial(scene, dst_x, dst_y, format, quality, key) {
        const ptr0 = passArray8ToWasm0(scene, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(format, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(key, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_rasterizePartial(this.__wbg_ptr, ptr0, len0, dst_x, dst_y, ptr1, len1, quality, ptr2, len2);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        var v4 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v4;
    }
    /**
     * Replay a `recordPdfScenes` envelope into a Krilla document, one scene per page.
     * Pages use a point-sized MediaBox, px-to-pt scale, and optional white fill.
     * Margins and `render_h` set paint bounds. Failures return empty bytes; read
     * `lastRasterizeInfo` for diagnostics.
     * @param {Uint8Array} envelope
     * @param {number} paper_width
     * @param {number} paper_height
     * @param {boolean} omit_background
     * @param {number} margin_top
     * @param {number} margin_right
     * @param {number} margin_bottom
     * @param {number} margin_left
     * @returns {Uint8Array}
     */
    renderPdf(envelope, paper_width, paper_height, omit_background, margin_top, margin_right, margin_bottom, margin_left) {
        const ptr0 = passArray8ToWasm0(envelope, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_renderPdf(this.__wbg_ptr, ptr0, len0, paper_width, paper_height, omit_background, margin_top, margin_right, margin_bottom, margin_left);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        var v2 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v2;
    }
    /**
     * Reserve scratch-buffer capacity for a `w`-by-`h` frame before rasterizing.
     * This does not create or replace a retained frame.
     * @param {number} w
     * @param {number} h
     */
    reserveFrame(w, h) {
        __wbg_call_guard();
        try {
            wasm.scenerasterizer_reserveFrame(this.__wbg_ptr, w, h);
        } catch(e) {
            __wbg_handle_catch(e);
        }
    }
    /**
     * Drop every cached font and image. Batch entries are stored under the hash the page
     * reports, so the host calls this when the document that supplies them changes.
     */
    resetResources() {
        __wbg_call_guard();
        try {
            wasm.scenerasterizer_resetResources(this.__wbg_ptr);
        } catch(e) {
            __wbg_handle_catch(e);
        }
    }
    /**
     * Key of the currently retained base frame ("" = none). The JS
     * side gates its baseSeq handshake on this before opting in.
     * @returns {string}
     */
    retainedFrameKey() {
        let deferred1_0;
        let deferred1_1;
        try {
            let ret;
            __wbg_call_guard();
            try {
                ret = wasm.scenerasterizer_retainedFrameKey(this.__wbg_ptr);
            } catch(e) {
                __wbg_handle_catch(e);
            }
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            __wbg_call_guard();
            try {
                wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
            } catch(e) {
                __wbg_handle_catch(e);
            }
        }
    }
    /**
     * Move the retained frame's pixels by (dx, dy) for a page scroll,
     * without encoding. The caller patches the exposed band with
     * `blitPartial`, then ships the frame with `encodeRetained`.
     * Returns `false` when `key` does not identify the retained frame.
     * @param {string} key
     * @param {number} dx
     * @param {number} dy
     * @returns {boolean}
     */
    shiftRetained(key, dx, dy) {
        const ptr0 = passStringToWasm0(key, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_shiftRetained(this.__wbg_ptr, ptr0, len0, dx, dy);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        return ret !== 0;
    }
    /**
     * Start a JPEG capture without retaining a full-frame pixel buffer.
     * @param {Uint8Array} scene
     * @param {number} quality
     * @returns {SceneJpegStream}
     */
    startJpeg(scene, quality) {
        const ptr0 = passArray8ToWasm0(scene, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        let ret;
        __wbg_call_guard();
        try {
            ret = wasm.scenerasterizer_startJpeg(this.__wbg_ptr, ptr0, len0, quality);
        } catch(e) {
            __wbg_handle_catch(e);
        }
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return SceneJpegStream.__wrap(ret[0]);
    }
}
if (Symbol.dispose) SceneRasterizer.prototype[Symbol.dispose] = SceneRasterizer.prototype.free;

export class StreamingPdf {
    static __wrap(ptr) {
        const obj = Object.create(StreamingPdf.prototype);
        obj.__wbg_ptr = ptr;
        StreamingPdfFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        StreamingPdfFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        __wbg_call_guard();
        try {
            wasm.__wbg_streamingpdf_free(ptr, 0);
        } catch(e) {
            __wbg_handle_catch(e);
        }

    }
}
if (Symbol.dispose) StreamingPdf.prototype[Symbol.dispose] = StreamingPdf.prototype.free;

/**
 * @param {string} op
 * @param {number} bytes
 * @param {string} detail
 * @returns {number}
 */
export function ks_js_span_enter(op, bytes, detail) {
    const ptr0 = passStringToWasm0(op, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passStringToWasm0(detail, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len1 = WASM_VECTOR_LEN;
    let ret;
    __wbg_call_guard();
    try {
        ret = wasm.ks_js_span_enter(ptr0, len0, bytes, ptr1, len1);
    } catch(e) {
        __wbg_handle_catch(e);
    }
    return ret >>> 0;
}

/**
 * @param {number} slot
 */
export function ks_js_span_exit(slot) {
    __wbg_call_guard();
    try {
        wasm.ks_js_span_exit(slot);
    } catch(e) {
        __wbg_handle_catch(e);
    }
}

/**
 * @param {Function | null} [f]
 */
export function ks_set_emit_hook(f) {
    __wbg_call_guard();
    try {
        wasm.ks_set_emit_hook(isLikeNone(f) ? 0 : addToExternrefTable0(f));
    } catch(e) {
        __wbg_handle_catch(e);
    }
}

/**
 * @param {string} span_id_hex
 */
export function set_ks_trace_parent(span_id_hex) {
    const ptr0 = passStringToWasm0(span_id_hex, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    __wbg_call_guard();
    try {
        wasm.set_ks_trace_parent(ptr0, len0);
    } catch(e) {
        __wbg_handle_catch(e);
    }
}

/**
 * @param {boolean} enabled
 */
export function set_ks_tracing(enabled) {
    __wbg_call_guard();
    try {
        wasm.set_ks_tracing(enabled);
    } catch(e) {
        __wbg_handle_catch(e);
    }
}

export function start() {
    __wbg_call_guard();
    try {
        wasm.start();
    } catch(e) {
        __wbg_handle_catch(e);
    }
}
function __wbg_get_imports() {
    const import0 = {
        __proto__: null,
        __wbg___wbindgen_is_function_5e4570eb24ffa122: function(arg0) {
            const ret = typeof(arg0) === 'function';
            return ret;
        },
        __wbg___wbindgen_is_object_a2790eb24c211ea0: function(arg0) {
            const val = arg0;
            const ret = typeof(val) === 'object' && val !== null;
            return ret;
        },
        __wbg___wbindgen_is_string_e6f02f0ea5f20a32: function(arg0) {
            const ret = typeof(arg0) === 'string';
            return ret;
        },
        __wbg___wbindgen_is_undefined_6cff064c44e0d823: function(arg0) {
            const ret = arg0 === undefined;
            return ret;
        },
        __wbg___wbindgen_panic_error_f153dd2564290663: function(arg0) {
            const ret = new PanicError(arg0);
            return ret;
        },
        __wbg___wbindgen_rethrow_fbd2dcd7d2b9ac5f: function(arg0) {
            throw new WebAssembly.Exception(__wbindgen_wrapped_jstag, [arg0]);
        },
        __wbg___wbindgen_throw_bb96b2010945f0bc: function(arg0, arg1) {
            throw new WebAssembly.Exception(__wbindgen_wrapped_jstag, [new Error(getStringFromWasm0(arg0, arg1))]);
        },
        __wbg_call_35dba3c747ad7521: function(arg0, arg1, arg2) {
            const ret = arg0.call(arg1, arg2);
            return ret;
        },
        __wbg_crypto_38df2bab126b63dc: function(arg0) {
            const ret = arg0.crypto;
            return ret;
        },
        __wbg_error_757e9472f8410341: function(arg0, arg1) {
            let deferred0_0;
            let deferred0_1;
            try {
                deferred0_0 = arg0;
                deferred0_1 = arg1;
                console.error(getStringFromWasm0(arg0, arg1));
            } finally {
                __wbg_call_guard();
                try {
                    wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
                } catch(e) {
                    __wbg_handle_catch(e);
                }
            }
        },
        __wbg_getRandomValues_a608c4436c19407a: function(arg0, arg1) {
            globalThis.crypto.getRandomValues(getArrayU8FromWasm0(arg0, arg1));
        },
        __wbg_getRandomValues_c44a50d8cfdaebeb: function(arg0, arg1) {
            arg0.getRandomValues(arg1);
        },
        __wbg_length_36bd29c6848c2144: function(arg0) {
            const ret = arg0.length;
            return ret;
        },
        __wbg_log_7da5b15cb08d0c7c: function(arg0, arg1) {
            console.log(getStringFromWasm0(arg0, arg1));
        },
        __wbg_msCrypto_bd5a034af96bcba6: function(arg0) {
            const ret = arg0.msCrypto;
            return ret;
        },
        __wbg_new_227d7c05414eb861: function() {
            const ret = new Error();
            return ret;
        },
        __wbg_new_with_length_3ffc1c56427c525c: function(arg0) {
            const ret = new Uint8Array(arg0 >>> 0);
            return ret;
        },
        __wbg_node_84ea875411254db1: function(arg0) {
            const ret = arg0.node;
            return ret;
        },
        __wbg_now_8b265300afd5f2b9: function() {
            const ret = Date.now();
            return ret;
        },
        __wbg_now_e7c6795a7f81e10f: function(arg0) {
            const ret = arg0.now();
            return ret;
        },
        __wbg_performance_3fcf6e32a7e1ed0a: function(arg0) {
            const ret = arg0.performance;
            return ret;
        },
        __wbg_process_44c7a14e11e9f69e: function(arg0) {
            const ret = arg0.process;
            return ret;
        },
        __wbg_prototypesetcall_de8e0d9553586985: function(arg0, arg1, arg2) {
            Uint8Array.prototype.set.call(getArrayU8FromWasm0(arg0, arg1), arg2);
        },
        __wbg_randomFillSync_6c25eac9869eb53c: function(arg0, arg1) {
            arg0.randomFillSync(arg1);
        },
        __wbg_require_b4edbdcf3e2a1ef0: function() {
            const ret = module.require;
            return ret;
        },
        __wbg_stack_3b0d974bbf31e44f: function(arg0, arg1) {
            const ret = arg1.stack;
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg_static_accessor_GLOBAL_THIS_466428f93b4eaa76: function() {
            const ret = typeof globalThis === 'undefined' ? null : globalThis;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_GLOBAL_c7aea38d4de089bc: function() {
            const ret = typeof global === 'undefined' ? null : global;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_SELF_42d4fae05e59267a: function() {
            const ret = typeof self === 'undefined' ? null : self;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_WINDOW_e0db14a0eba6a812: function() {
            const ret = typeof window === 'undefined' ? null : window;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_subarray_a4cc58201c7359fd: function(arg0, arg1, arg2) {
            const ret = arg0.subarray(arg1 >>> 0, arg2 >>> 0);
            return ret;
        },
        __wbg_versions_276b2795b1c6a219: function(arg0) {
            const ret = arg0.versions;
            return ret;
        },
        __wbg_warn_3efc74ed1d5e71bd: function(arg0, arg1) {
            console.warn(getStringFromWasm0(arg0, arg1));
        },
        __wbindgen_cast_0000000000000001: function(arg0, arg1) {
            // Cast intrinsic for `Ref(Slice(U8)) -> NamedExternref("Uint8Array")`.
            const ret = getArrayU8FromWasm0(arg0, arg1);
            return ret;
        },
        __wbindgen_cast_0000000000000002: function(arg0, arg1) {
            // Cast intrinsic for `Ref(String) -> Externref`.
            const ret = getStringFromWasm0(arg0, arg1);
            return ret;
        },
        __wbindgen_init_externref_table: function() {
            const table = wasm.__wbindgen_externrefs;
            const offset = table.grow(4);
            table.set(0, undefined);
            table.set(offset + 0, undefined);
            table.set(offset + 1, null);
            table.set(offset + 2, true);
            table.set(offset + 3, false);
        },
        __wbindgen_jstag: WebAssembly.JSTag,
        __wbindgen_wrapped_jstag: __wbindgen_wrapped_jstag,
    };
    return {
        __proto__: null,
        "./renderer_core_bg.js": import0,
    };
}

const __wbindgen_wrapped_jstag = new WebAssembly.Tag({ parameters: ['externref'] });


let __wbg_terminated_addr;
let __wbg_called_abort = false;
function __wbg_call_abort_hook() {
    __wbg_called_abort = true;
    try {
        const idx = getInt32ArrayMemory0()[wasm.__abort_handler.value / 4];
        if (idx) wasm.__wbindgen_export.get(idx)();
    } catch(_) {}
}

function __wbg_handle_catch(e) {
    if (e instanceof WebAssembly.Exception && e.is(__wbindgen_wrapped_jstag)) {
        throw e.getArg(__wbindgen_wrapped_jstag, 0);
    }
    getInt32ArrayMemory0()[__wbg_terminated_addr] = 1;
    __wbg_call_abort_hook();
    throw e;
}


function __wbg_call_guard() {
    __wbg_terminated_addr ??= wasm.__instance_terminated.value / 4;
    const flag = getInt32ArrayMemory0()[__wbg_terminated_addr];
    if (flag) {
        if (!__wbg_called_abort) {
            __wbg_call_abort_hook();
        }throw new Error('Module terminated');
    }
}
const SceneJpegStreamFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_scenejpegstream_free(ptr, 1));
const SceneRasterizerFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_scenerasterizer_free(ptr, 1));
const StreamingPdfFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_streamingpdf_free(ptr, 1));

function addToExternrefTable0(obj) {
    const idx = wasm.__externref_table_alloc();
    wasm.__wbindgen_externrefs.set(idx, obj);
    return idx;
}

function _assertClass(instance, klass) {
    if (!(instance instanceof klass)) {
        throw new Error(`expected instance of ${klass.name}`);
    }
}

function getArrayJsValueFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    const mem = getDataViewMemory0();
    const result = [];
    for (let i = ptr; i < ptr + 4 * len; i += 4) {
        result.push(wasm.__wbindgen_externrefs.get(mem.getUint32(i, true)));
    }
    wasm.__externref_drop_slice(ptr, len);
    return result;
}

function getArrayU8FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getUint8ArrayMemory0().subarray(ptr / 1, ptr / 1 + len);
}

let cachedDataViewMemory0 = null;
function getDataViewMemory0() {
    if (cachedDataViewMemory0 === null || cachedDataViewMemory0.buffer.detached === true || (cachedDataViewMemory0.buffer.detached === undefined && cachedDataViewMemory0.buffer !== wasm.memory.buffer)) {
        cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
    }
    return cachedDataViewMemory0;
}

let cachedInt32ArrayMemory0 = null;
function getInt32ArrayMemory0() {
    if (cachedInt32ArrayMemory0 === null || cachedInt32ArrayMemory0.byteLength === 0) {
        cachedInt32ArrayMemory0 = new Int32Array(wasm.memory.buffer);
    }
    return cachedInt32ArrayMemory0;
}

function getStringFromWasm0(ptr, len) {
    return decodeText(ptr >>> 0, len);
}

let cachedUint8ArrayMemory0 = null;
function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

function isLikeNone(x) {
    return x === undefined || x === null;
}

class PanicError extends Error {}
Object.defineProperty(PanicError.prototype, 'name', {
    value: 'PanicError',
});

function passArray8ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 1, 1) >>> 0;
    getUint8ArrayMemory0().set(arg, ptr / 1);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}

function passStringToWasm0(arg, malloc, realloc) {
    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8ArrayMemory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8ArrayMemory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }
    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
        const ret = cachedTextEncoder.encodeInto(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

function takeFromExternrefTable0(idx) {
    const value = wasm.__wbindgen_externrefs.get(idx);
    wasm.__externref_table_dealloc(idx);
    return value;
}

let cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
cachedTextDecoder.decode();
const MAX_SAFARI_DECODE_BYTES = 2146435072;
let numBytesDecoded = 0;
function decodeText(ptr, len) {
    numBytesDecoded += len;
    if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
        cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
        cachedTextDecoder.decode();
        numBytesDecoded = len;
    }
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

const cachedTextEncoder = new TextEncoder();

if (!('encodeInto' in cachedTextEncoder)) {
    cachedTextEncoder.encodeInto = function (arg, view) {
        const buf = cachedTextEncoder.encode(arg);
        view.set(buf);
        return {
            read: arg.length,
            written: buf.length
        };
    };
}

let WASM_VECTOR_LEN = 0;

let wasmModule, wasmInstance, wasm;
function __wbg_finalize_init(instance, module) {
    wasmInstance = instance;
    wasm = instance.exports;
    wasmModule = module;
    cachedDataViewMemory0 = null;
    cachedInt32ArrayMemory0 = null;
    cachedUint8ArrayMemory0 = null;
    wasm.__wbindgen_start();
    return wasm;
}

async function __wbg_load(module, imports) {
    if (typeof Response === 'function' && module instanceof Response) {
        if (!module.ok) {
            throw new Error(`failed to fetch Wasm: ${module.status} ${module.statusText} fetching '${module.url}'`);
        }

        if (typeof WebAssembly.instantiateStreaming === 'function') {
            try {
                return await WebAssembly.instantiateStreaming(module, imports);
            } catch (e) {
                const validResponse = expectedResponseType(module.type);

                if (validResponse && module.headers.get('Content-Type') !== 'application/wasm') {
                    console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);

                } else { throw e; }
            }
        }

        const bytes = await module.arrayBuffer();
        return await WebAssembly.instantiate(bytes, imports);
    } else {
        const instance = await WebAssembly.instantiate(module, imports);

        if (instance instanceof WebAssembly.Instance) {
            return { instance, module };
        } else {
            return instance;
        }
    }

    function expectedResponseType(type) {
        switch (type) {
            case 'basic': case 'cors': case 'default': return true;
        }
        return false;
    }
}

function initSync(module) {
    if (wasm !== undefined) return wasm;


    if (module !== undefined) {
        if (Object.getPrototypeOf(module) === Object.prototype) {
            ({module} = module)
        } else {
            console.warn('using deprecated parameters for `initSync()`; pass a single object instead')
        }
    }

    const imports = __wbg_get_imports();
    if (!(module instanceof WebAssembly.Module)) {
        module = new WebAssembly.Module(module);
    }
    const instance = new WebAssembly.Instance(module, imports);
    return __wbg_finalize_init(instance, module);
}

async function __wbg_init(module_or_path) {
    if (wasm !== undefined) return wasm;


    if (module_or_path !== undefined) {
        if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
            ({module_or_path} = module_or_path)
        } else {
            console.warn('using deprecated parameters for the initialization function; pass a single object instead')
        }
    }

    if (module_or_path === undefined) {
        module_or_path = new URL('renderer_core_bg.wasm', import.meta.url);
    }
    const imports = __wbg_get_imports();

    if (typeof module_or_path === 'string' || (typeof Request === 'function' && module_or_path instanceof Request) || (typeof URL === 'function' && module_or_path instanceof URL)) {
        module_or_path = fetch(module_or_path);
    }

    const { instance, module } = await __wbg_load(await module_or_path, imports);

    return __wbg_finalize_init(instance, module);
}

export { initSync, __wbg_init as default };
