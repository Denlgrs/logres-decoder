import argparse
import pandas as pd

from parser import parse_messages, expand, ByteParser


LOGRES_PORT = 8800


def load_byte_stream(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # hex -> bytes
    df["payload_bytes"] = df["payload_hex"].map(bytes.fromhex)

    # 通信方向
    df["direction"] = df["src_port"].apply(
        lambda port: "RECV" if port == LOGRES_PORT else "SEND"
    )

    return df


def load_function_ids(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    df["func_id_bytes"] = df["func_id"].map(
        lambda f_id: int.to_bytes(int(f_id, 16), 4, byteorder="little")
    )

    return df


def parse_byte_streams(df: pd.DataFrame) -> pd.DataFrame:
    """
    バイトストリームをメッセージにパースする
    """
    parsed_results = []

    for direction, group in df.groupby("direction"):
        # 同一direction内で連結
        raw_bytes = b"".join(group["payload_bytes"].tolist())

        messages, _ = parse_messages(raw_bytes)
        parsed = pd.DataFrame(messages)

        # body展開
        parsed["body"] = parsed["body"].map(expand)

        # directionを付与（ここで初めて付ける）
        parsed["direction"] = direction

        parsed_results.append(parsed)

    # 全方向を結合
    return pd.concat(parsed_results, ignore_index=True)


def parse_function_bodies(
    mapped_df: pd.DataFrame, func_id_df: pd.DataFrame
) -> pd.DataFrame:
    parser = ByteParser(
        func_id_df,
        funcs_path="data/funcs.json",
        objects_path="data/objects.json",
    )

    def safe_parse(row):
        try:
            return parser.parse_message_body(row["body"])
        except Exception as e:
            print(f"[WARN] parse failed: {e}")
            return []

    result = mapped_df.copy()

    # 各行で「複数メッセージ」を取得
    result["parsed_list"] = result.apply(safe_parse, axis=1)

    # explodeで1メッセージ1行に展開
    result = result.explode("parsed_list").reset_index(drop=True)

    # dictを列に展開
    parsed_expanded = pd.json_normalize(result["parsed_list"])

    result = pd.concat([result.drop(columns=["parsed_list"]), parsed_expanded], axis=1)

    return result


def main():
    parser = argparse.ArgumentParser(description="ログレス通信解析ツール")
    parser.add_argument("input_csv", help="入力CSVファイル")
    parser.add_argument(
        "--func-id-csv", default="data/func_id.csv", help="関数ID定義CSV"
    )
    parser.add_argument(
        "--output", default="out/parsed_output.csv", help="出力CSVファイル"
    )

    args = parser.parse_args()

    # --- 処理パイプライン ---
    stream_df = load_byte_stream(args.input_csv)
    func_id_df = load_function_ids(args.func_id_csv)

    message_df = parse_byte_streams(stream_df)
    final_df = parse_function_bodies(message_df, func_id_df)

    # 必要な列だけ保存
    output_df = final_df[["direction", "func_name", "parsed"]]

    # pingは多いので削除
    output_df = output_df[output_df["func_name"] != "ping"]
    output_df = output_df[output_df["func_name"] != "ping_Response"]

    output_df.to_csv(args.output, index=False)

    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
