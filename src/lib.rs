#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(all(target_arch = "wasm32", target_os = "wasi"))]
fn getrandom_inner(dest: &mut [u8]) -> Result<(), getrandom::Error> {
    // SAFETY: This unsafe block is required to call the WASI-specific random number generation function.
    // The function `_wasi_random_get` is provided by the WASI runtime and is guaranteed to be available
    // when running in a WASI environment. The pointer and length are valid as they come directly from
    // the mutable slice, and the WASI function will not access memory beyond the provided length.
    let ret = unsafe { core::arch::wasm32::_wasi_random_get(dest.as_mut_ptr(), dest.len()) };
    if ret == 0 {
        Ok(())
    } else {
        Err(getrandom::Error::UNAVAILABLE)
    }
}

// Full WASM bindings for datasetq using dsq-filter for query processing
#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
#[wasm_bindgen]
pub fn process_datasetq_query(query: &str, data_json: &str) -> Result<String, JsValue> {
    use dsq_filter::execute_filter;
    use dsq_shared::value::Value;

    // Parse the JSON data into Value
    let data: serde_json::Value = serde_json::from_str(data_json)
        .map_err(|e| JsValue::from_str(&format!("Failed to parse JSON data: {}", e)))?;

    let value = Value::from_json(data);

    // Execute the query using dsq-filter
    let result = execute_filter(query, &value)
        .map_err(|e| JsValue::from_str(&format!("Failed to execute query: {}", e)))?;

    // Convert result back to JSON
    let json_result = result
        .to_json()
        .map_err(|e| JsValue::from_str(&format!("Failed to convert result to JSON: {}", e)))?;

    serde_json::to_string(&json_result)
        .map_err(|e| JsValue::from_str(&format!("Failed to serialize JSON: {}", e)))
}

// Load parquet data from bytes and return as JSON
#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
#[wasm_bindgen]
pub fn load_parquet_data(_parquet_bytes: &[u8]) -> Result<String, JsValue> {
    Err(JsValue::from_str("Parquet support not available on WASM"))
}

// Keep the placeholder greet function for now
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn greet(name: &str) -> String {
    format!("Hello, {}!", name)
}

// Custom qsort_r implementation for WASM
// SAFETY: This function is intentionally unsafe as it performs raw pointer manipulation
// for implementing quicksort. Callers must ensure:
// 1. `base` points to a valid array of `nmemb` elements, each of size `size` bytes
// 2. The memory region `[base, base + nmemb * size)` is valid and properly aligned
// 3. The `compar` function safely compares elements without side effects
// 4. The `arg` pointer is valid for the lifetime of the sort operation
// 5. No other code accesses the memory region during the sort
#[cfg(target_arch = "wasm32")]
pub unsafe fn qsort_r(
    base: *mut u8,
    nmemb: usize,
    size: usize,
    compar: fn(*const u8, *const u8, *mut u8) -> i32,
    arg: *mut u8,
) {
    if nmemb <= 1 {
        return;
    }

    let pivot_index = nmemb / 2;
    let pivot = base.add(pivot_index * size);
    let end = base.add((nmemb - 1) * size);

    // Swap pivot and end
    for i in 0..size {
        std::ptr::swap(pivot.add(i), end.add(i));
    }

    let mut i = 0;
    for j in 0..nmemb - 1 {
        let elem = base.add(j * size);
        if compar(elem, end, arg) < 0 {
            if i != j {
                for k in 0..size {
                    std::ptr::swap(base.add(i * size + k), base.add(j * size + k));
                }
            }
            i += 1;
        }
    }

    // Swap i and end
    for k in 0..size {
        std::ptr::swap(base.add(i * size + k), end.add(k));
    }

    qsort_r(base, i, size, compar, arg);
    qsort_r(base.add((i + 1) * size), nmemb - i - 1, size, compar, arg);
}
