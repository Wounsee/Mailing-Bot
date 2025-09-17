# imghdr.py — минимальная замена stdlib imghdr.what
# Поддерживает: jpeg, png, gif, webp, bmp

def _starts_with(h, sigs):
    return any(h.startswith(s) for s in sigs)

def what(file=None, h=None):
    """
    Возвращает тип изображения: 'jpeg','png','gif','webp','bmp' или None.
    file: путь к файлу или файловый объект; h: первые байты.
    """
    data = b""
    if h is not None:
        data = h if isinstance(h, (bytes, bytearray)) else bytes(h)
    elif file is not None:
        try:
            # file может быть именем файла или файловым объектом
            if hasattr(file, "read"):
                pos = file.tell() if hasattr(file, "tell") else None
                data = file.read(64)
                try:
                    if pos is not None:
                        file.seek(pos)
                except Exception:
                    pass
            else:
                with open(file, "rb") as f:
                    data = f.read(64)
        except Exception:
            return None
    else:
        return None

    if not data:
        return None

    # JPEG
    if data.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    # PNG
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    # GIF
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "gif"
    # WEBP (RIFF....WEBP)
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    # BMP
    if data.startswith(b"BM"):
        return "bmp"
    return None
