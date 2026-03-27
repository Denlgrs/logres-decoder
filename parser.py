import json
import struct
from typing import Any, Tuple, Dict, List


def encode_value(type_name: str, value: Any) -> bytes:
    if type_name == "float":
        return struct.pack("<f", value)

    if type_name == "unsigned int":
        return struct.pack("<I", value)

    if type_name == "int":
        return struct.pack("<i", value)

    if type_name == "__int64":
        return struct.pack("<q", value)

    if type_name == "unsigned __int64":
        return struct.pack("<Q", value)

    if type_name == "bool":
        return struct.pack("<?", value)

    if type_name == "string":
        raw = value.encode("utf-8")
        return struct.pack("<I", len(raw)) + raw

    if type_name == "unsigned char":
        return struct.pack("<B", value)

    if type_name == "List":
        buf = struct.pack("<I", len(value))  # 要素数
        for elem in value:
            [(elem_type, elem_value)] = elem.items()
            buf += encode_value(elem_type, elem_value)
        return buf

    # 構造体（t_WarpUID など）
    if isinstance(value, list):
        buf = b""
        for elem in value:
            [(sub_type, sub_value)] = elem.items()
            buf += encode_value(sub_type, sub_value)
        return buf

    raise ValueError(f"Unsupported type: {type_name}")


def encode_message(parsed: List[Dict[str, Dict[str, Any]]]) -> bytes:
    buf = b""

    for field in parsed:
        [(_, type_dict)] = field.items()
        [(type_name, value)] = type_dict.items()

        buf += encode_value(type_name, value)

    return buf


def read_int_le(data: bytes, offset: int) -> tuple[int, int]:
    """
    little-endian int32 を読む
    戻り値: (値, 次の offset)
    """
    value = int.from_bytes(data[offset : offset + 4], "little", signed=True)
    return value, offset + 4


def read_unsigned_int_le(data: bytes, offset: int) -> tuple[int, int]:
    """
    little-endian int32 を読む
    戻り値: (値, 次の offset)
    """
    value = int.from_bytes(data[offset : offset + 4], "little", signed=False)
    return value, offset + 4


def read_unsigned_int64_le(data: bytes, offset: int) -> tuple[int, int]:
    """
    little-endian 64bit 整数を読む
    戻り値: (値, 次の offset)
    """
    value = int.from_bytes(data[offset : offset + 8], "little", signed=False)
    return value, offset + 8


def read_int64_le(data: bytes, offset: int) -> tuple[int, int]:
    """
    little-endian 64bit 整数を読む
    戻り値: (値, 次の offset)
    """
    value = int.from_bytes(data[offset : offset + 8], "little", signed=True)
    return value, offset + 8


def read_float_le(data: bytes, offset: int) -> tuple[float, int]:
    """
    little-endian float32 を読む（IEEE754）

    戻り値
    -------
    value : float
            読み取った値
    next_offset : int
            次に読むオフセット
    """
    value = struct.unpack_from("<f", data, offset)[0]
    return value, offset + 4


def read_string_le(data: bytes, offset: int) -> tuple[str, int]:
    """
    little-endian 4バイト長 + UTF-8 文字列を読む
    """
    # まず長さを読む
    str_len, offset = read_int_le(data, offset)

    # 文字列本体を読む
    str_bytes = data[offset : offset + str_len]
    value = str_bytes.decode("utf-8", errors="replace")

    return value, offset + str_len


def read_bool(data: bytes, offset: int) -> tuple[bool, int]:
    """
    1バイト bool を読む
    戻り値: (値, 次の offset)
    """
    value = data[offset] != 0
    return value, offset + 1


def read_unsigned_char(data: bytes, offset: int) -> tuple[int, int]:
    """
    unsigned char (1バイト) を読む
    戻り値: (値, 次の offset)
    """
    value = data[offset]  # 0〜255 の整数として取得される
    return value, offset + 1


def read_unsigned_char_array(data: bytes, offset: int) -> tuple[bytes, int]:
    """
    長さ付き List[unsigned char] を読む（<I length + raw bytes）
    """
    length, offset = read_unsigned_int_le(data, offset)
    value = data[offset : offset + length]
    return value, offset + length


TYPE_DECODERS = {
    "int": read_int_le,
    "unsigned int": read_unsigned_int_le,
    "float": read_float_le,
    "string": read_string_le,
    "__int64": read_int64_le,
    "unsigned __int64": read_unsigned_int64_le,
    "bool": read_bool,
}


def read_array_le(
    data: bytes,
    offset: int,
    element_type: str,
    objects: dict,
    max_count: int = 1000,
) -> tuple[list, int]:
    """
    little-endian 4byte 要素数 + 配列本体を読む
    element_type はプリミティブか struct/class 名
    要素数が max_count を超えたら例外を投げる
    """
    # 要素数を取得
    count, offset = read_int_le(data, offset)

    if count < 0 or count > max_count:
        raise ValueError(f"array size {count} is invalid (max allowed: {max_count})")

    values = []

    for _ in range(count):
        # 各要素を parse_value_by_type で再帰的に読み込む
        elem, offset = parse_value_by_type(data, offset, element_type, objects)
        values.append(elem)

    return values, offset


def parse_value_by_type(
    data: bytes,
    offset: int,
    type_name: str,
    objects: dict,
) -> tuple[dict, int]:
    """
    型名に基づいて値を1つパースする（再帰対応）
    戻り値: ({type_name: value}, next_offset)
    """

    # 特例：List[unsigned char] → バイト列として読む
    if type_name == "List[unsigned char]":
        value, offset = read_unsigned_char_array(data, offset)
        return {type_name: value}, offset

    # 配列 / List<T> 判定
    if type_name.startswith("List[") and type_name.endswith("]"):
        element_type = type_name[5:-1]  # List[...] の中身
        values, offset = read_array_le(data, offset, element_type, objects)
        return {"List": values}, offset

    # プリミティブ型
    if type_name in TYPE_DECODERS:
        decoder = TYPE_DECODERS[type_name]
        value, offset = decoder(data, offset)
        return {type_name: value}, offset

    # struct / class（完全修飾名・短縮名どちらも許容）
    short_name = type_name.split("::")[-1]
    if short_name in objects:
        values, offset = parse_struct(data, offset, short_name, objects)
        return {short_name: values}, offset

    raise NotImplementedError(f"Unsupported type: {type_name}")


def parse_struct(
    data: bytes,
    offset: int,
    struct_name: str,
    objects: dict,
) -> tuple[list[dict], int]:
    """
    objects.json に従って struct / class を再帰的にパースする
    """
    struct_def = objects[struct_name]["arguments"]
    values = []

    for field_type in struct_def:
        parsed, offset = parse_value_by_type(data, offset, field_type, objects)
        values.append(parsed)

    return values, offset


def parse_content_by_signature(
    content: bytes,
    arg_names: list[str],
    arg_types: list[str],
    objects: dict,
) -> list[dict]:
    """
    content を (引数名, 引数型) に従って解析する
    """
    offset = 0
    result = []

    for arg_name, arg_type in zip(arg_names, arg_types):
        parsed, offset = parse_value_by_type(content, offset, arg_type, objects)
        # parsed は {type_name: value}
        result.append({arg_name: parsed})

    return result


def read_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    """
    LEB128 / varint (7bit, little endian) を読む。
    戻り値: (値, 次のoffset)
    """
    value = 0
    shift = 0

    while True:
        b = data[offset]
        offset += 1

        value |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break

        shift += 7

    return value, offset


class Buffer:
    def __init__(self, data: bytes):
        self.buf = bytearray(data)
        self.start = 0
        self.cur = 0
        self.end = len(self.buf)

    def expand(self, outbuf, arr_len_table, arr_mask_table):
        data = self.buf
        data_addr = self.cur

        while data_addr < self.end:
            control_byte = data[data_addr]
            flag = control_byte & 0x3
            data_addr += 1  # control_byte消費

            # -------------------------
            # リテラルコピー
            # -------------------------
            if flag == 0:
                literal_length = (control_byte >> 2) + 1

                # 小サイズ直コピー
                if literal_length <= 0x10:
                    if data_addr + literal_length > self.end:
                        break

                    outbuf.buf[outbuf.cur : outbuf.cur + literal_length] = data[
                        data_addr : data_addr + literal_length
                    ]

                    outbuf.cur += literal_length
                    data_addr += literal_length
                    continue

                # 拡張長さ
                if literal_length >= 0x3D:
                    i = literal_length - 0x3C
                    mask = arr_mask_table[i]

                    value = int.from_bytes(data[data_addr : data_addr + 4], "little")
                    literal_length = (value & mask) + 1
                    data_addr += i

                if data_addr + literal_length > self.end:
                    break

                outbuf.buf[outbuf.cur : outbuf.cur + literal_length] = data[
                    data_addr : data_addr + literal_length
                ]

                outbuf.cur += literal_length
                data_addr += literal_length
                continue

            # -------------------------
            # 後方参照
            # -------------------------
            else:
                var = arr_len_table[control_byte]
                length = var & 0xFF
                i = var >> 11
                extra = var & 0x700

                mask = arr_mask_table[i]
                value = int.from_bytes(data[data_addr : data_addr + 4], "little")
                distance = (value & mask) + extra

                data_addr += i

                if distance == 0 or outbuf.cur < distance:
                    return False

                src = outbuf.cur - distance
                dst = outbuf.cur

                if length <= 0x10 and distance >= 8:
                    outbuf.buf[dst : dst + length] = outbuf.buf[src : src + length]
                else:
                    for j in range(length):
                        outbuf.buf[dst + j] = outbuf.buf[src + j]

                outbuf.cur += length
                continue

        self.cur = data_addr
        return True


def expand(body):
    flag = body[0]
    body = body[1:]  # flag消費
    if flag == 0:
        return body

    size, offset = read_varint(body)
    body = body[offset:]

    intput_buf = Buffer(body)
    out_buf = Buffer(bytes(size))

    intput_buf.expand(
        out_buf, arr_len_table, arr_mask_table
    )  # out_buf.bufに展開後が入る

    return bytes(out_buf.buf)


with open("data/table_data.bin", "rb") as f:
    table_data = f.read()

# 2バイトずつ little-endian WORD に変換
arr_len_table = list(struct.unpack("<" + "H" * (len(table_data) // 2), table_data))

arr_mask_table = [0, 0xFF, 0xFFFF, 0xFFFFFF, 0xFFFFFFFF]


def parse_messages(data: bytes) -> Tuple[List[dict], int]:
    """
    data に連結された複数メッセージを順にパースする

    Returns:
            messages: パース済みメッセージ
            offset: 最後に正常にパースできた位置（ここ以降は未処理）
    """
    messages = []
    offset = 0
    size = len(data)

    while offset < size:
        start_offset = offset

        # headerが読めるか
        if offset + 1 > size:
            break

        header = data[offset]
        offset += 1

        # msg_idが読めるか
        if offset + 4 > size:
            break

        msg_id = int.from_bytes(data[offset : offset + 4], "little")
        offset += 4

        # varintが最後まで読めるかは read_varint 側に依存
        try:
            body_len, offset = read_varint(data, offset)
        except Exception:
            # varint途中で切れている
            break

        # bodyが全部あるかチェック
        if offset + body_len > size:
            # まだ全部受信していないので戻る
            offset = start_offset
            break

        body = data[offset : offset + body_len]
        offset += body_len

        messages.append(
            {
                "offset": start_offset,
                "header": header,
                "message_id": msg_id,
                "body_length": body_len,
                "body": body,
            }
        )

    return messages, offset


def parse_sized_json(body: bytes):
    """
    [length 4B][json] の形式をパースし、
    func_name, parsed を返す
    """
    if len(body) < 4:
        raise ValueError("body too short")

    size = int.from_bytes(body[:4], "little")

    if len(body) < 4 + size:
        raise ValueError("incomplete json body")

    json_bytes = body[4 : 4 + size]
    obj = json.loads(json_bytes.decode("utf-8"))

    # 期待フォーマット:
    # {'args': {...}, 'procedure': 'ping'}
    func_name = obj.get("procedure")
    parsed = obj.get("args")

    return func_name, json.dumps(parsed)  # DFに変換するとおかしくなるので文字列に


class ByteParser:
    def __init__(
        self, df, funcs_path="data/funcs.json", objects_path="data/objects.json"
    ):
        # func_name -> func_id_bytes
        func_name_to_bytes = df.set_index("func_name")["func_id_bytes"].to_dict()

        # 逆引き: func_id_bytes -> func_name
        self.bytes_to_func_name = {v: k for k, v in func_name_to_bytes.items()}

        # funcs.json
        with open(funcs_path, "r", encoding="utf-8") as f:
            self.funcs = json.load(f)

        # objects.json
        with open(objects_path, "r", encoding="utf-8") as f:
            objs = json.load(f)
        self.objects = {d["name"]: {"arguments": d["arguments"]} for d in objs}

    def split_requests(self, body_bytes):
        """
        連結された複数リクエストを分割
        入力：[request_id 4B][length 4B][body]が複数連結されたバイト列
        """
        offset = 0
        results = []

        while offset < len(body_bytes):
            if len(body_bytes) < offset + 8:
                break

            format_id = body_bytes[offset : offset + 4]
            length = int.from_bytes(body_bytes[offset + 4 : offset + 8], "little")

            start = offset + 8
            end = start + length

            if len(body_bytes) < end:
                break

            body = body_bytes[start:end]
            results.append((format_id, body))

            offset = end

        return results

    def decode_request(self, format_id, body):
        if format_id == b"\x35\x84\x31\x31":
            return self.parse_func_bytes(body)
        else:
            return parse_sized_json(body)

    def parse_message_body(self, body_bytes):
        """
        Message bodyをRequest単位に分解してデコード
        """
        results = []

        for format_id, body in self.split_requests(body_bytes):
            try:
                func_name, parsed = self.decode_request(format_id, body)

                results.append(
                    {"format_id": format_id, "func_name": func_name, "parsed": parsed}
                )

            except Exception as e:
                print(f"[WARN] parse failed: {e}")
                results.append(
                    {"format_id": format_id, "func_name": None, "parsed": None}
                )

        return results

    def parse_func_bytes(self, func_bytes):
        """
        バイト列から関数を自動判定してパースする
        """
        # 先頭4バイト = func_id
        func_id_bytes = func_bytes[:4]

        # func_name を特定
        func_name = self.bytes_to_func_name.get(func_id_bytes)
        if func_name is None:
            raise ValueError(f"Unknown func_id: {func_id_bytes.hex()}")

        # content 抽出
        content = func_bytes[4:]

        # シグネチャ取得
        args = self.funcs[func_name].get("args", [])
        arg_names = [arg["name"] for arg in args]
        arg_types = [arg["type"] for arg in args]

        # パース
        parsed = parse_content_by_signature(
            content=content,
            arg_names=arg_names,
            arg_types=arg_types,
            objects=self.objects,
        )

        return func_name, parsed
