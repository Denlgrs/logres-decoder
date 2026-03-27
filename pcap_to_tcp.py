import csv
from scapy.all import PcapReader, TCP, Raw

# "C:\Program Files\Wireshark\tshark.exe" -i 5 -f "tcp port 8800" -w out/dump.pcap


if __name__ == "__main__":
    input_pcap = "out/dump.pcap"
    output_csv = "out/tcp_raw.csv"

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # ヘッダ
        writer.writerow(["src_ip", "src_port", "dst_ip", "dst_port", "payload_hex"])

        with PcapReader(input_pcap) as pcap:
            for pkt in pcap:
                if pkt.haslayer(TCP) and pkt.haslayer(Raw):
                    ip = pkt[0]
                    tcp = pkt[TCP]
                    payload = bytes(pkt[Raw].load)

                    # バイナリはそのままだと扱いづらいのでhex化
                    payload_hex = payload.hex()

                    writer.writerow([ip.src, tcp.sport, ip.dst, tcp.dport, payload_hex])

    print("保存完了")
