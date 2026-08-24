def join_storage_path(
    base: str,
    *parts: str,
) -> str:
    if not base:
        raise ValueError("Storage base path must not be empty")

    normalized_parts = [str(part).strip("/") for part in parts if str(part).strip("/")]

    if not normalized_parts:
        return base.rstrip("/") or "/"

    if base == "/":
        return "/" + "/".join(normalized_parts)

    return "/".join(
        [
            base.rstrip("/"),
            *normalized_parts,
        ]
    )
