import socket
import time
SERVER_IP = '10.21.31.103'
SERVER_PORT = 30001

def build_protocol_header(data_length: int, msg_id: int = 1, asdu_format: int = 0x01) -> bytearray:
    if not (0 <= data_length <= 65535):
        raise ValueError('data_length must be between 0 and 65535')
    if not (0 <= msg_id <= 65535):
        raise ValueError('msg_id must be between 0 and 65535')
    if asdu_format not in [0x01, 0x00]:
        raise ValueError('asdu_format must be 0x01 or 0x00')

    header = bytearray(16)
    header[0] = 0xeb
    header[1] = 0x91
    header[2] = 0xeb
    header[3] = 0x90
    header[4] = data_length & 0xFF
    header[5] = (data_length >> 8) & 0xFF
    header[6] = msg_id & 0xFF
    header[7] = (msg_id >> 8) & 0xFF
    header[8] = asdu_format
    return header

def receive_response(sock, timeout=5):
    """接收并解析响应数据包"""
    # 设置接收超时
    sock.settimeout(timeout)

    try:
        # 第一步：接收16字节的协议头部
        header_data = b''
        while len(header_data) < 16:
            chunk = sock.recv(16 - len(header_data))
            if not chunk:
                raise ConnectionError("连接已关闭")
            header_data += chunk

        # 解析协议头部
        header_info = parse_protocol_header(header_data)
        data_length = header_info['data_length']

        print(f"接收到响应头部: msg_id={header_info['msg_id']}, "
              f"data_length={data_length}, asdu_format={header_info['asdu_format']:02x}")

        # 第二步：根据头部中的长度接收ASDU数据
        asdu_data = b''
        if data_length > 0:
            while len(asdu_data) < data_length:
                chunk = sock.recv(data_length - len(asdu_data))
                if not chunk:
                    raise ConnectionError("连接已关闭")
                asdu_data += chunk

        # 尝试解析JSON数据
        try:
            if asdu_data:
                json_str = asdu_data.decode('utf-8')
                json_obj = json.loads(json_str)
                return {
                    'header': header_info,
                    'asdu_data': asdu_data,
                    'json_str': json_str,
                    'json_obj': json_obj,
                    'complete_message': header_data + asdu_data
                }
            else:
                return {
                    'header': header_info,
                    'asdu_data': asdu_data,
                    'complete_message': header_data
                }
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            # 如果不是JSON格式，返回原始数据
            return {
                'header': header_info,
                'asdu_data': asdu_data,
                'complete_message': header_data + asdu_data
            }

    except socket.timeout:
        print("接收响应超时")
        return None
    except Exception as e:
        print(f"接收响应时发生错误: {e}")
        return None

def parse_protocol_header(data: bytes) -> dict:
    """解析协议头部"""
    if len(data) < 16:
        raise ValueError("数据长度不足16字节，无法解析协议头部")

    # 检查起始标识
    if data[0] != 0xeb or data[1] != 0x91 or data[2] != 0xeb or data[3] != 0x90:
        raise ValueError("协议头部起始标识错误")

    # 解析字段
    data_length = data[4] + (data[5] << 8)
    msg_id = data[6] + (data[7] << 8)
    asdu_format = data[8]

    return {
        'data_length': data_length,
        'msg_id': msg_id,
        'asdu_format': asdu_format,
        'raw_header': data[:16]
    }

def send_and_receive_thread(sock, message):
    """发送数据并接收响应的线程函数"""
    print("发送接收线程启动...")

    # 解析发送的消息，获取头部信息（用于对比）
    send_header_info = parse_protocol_header(message[:16])

    try:
        # 发送数据
        send_len = sock.sendall(message)
        print(f"[{time.strftime('%H:%M:%S')}] 发送成功，msg_id={send_header_info['msg_id']}")

        # 接收响应
        response = receive_response(sock, timeout=30)

        if response:
            # 打印响应摘要
            print(f"[{time.strftime('%H:%M:%S')}] 收到响应，msg_id={response['header']['msg_id']}")

            # 如果响应中包含JSON数据，打印部分内容
            if 'json_str' in response:
                # 截取前200个字符显示
                json_preview = response['json_str'][:200]
                if len(response['json_str']) > 200:
                    json_preview += "..."
                print(f"响应JSON数据预览: {json_preview}")
                return response['json_obj']
            else:
                print(
                    f"响应ASDU数据（原始）: {response['asdu_data'][:100].hex() if response['asdu_data'] else '无数据'}")
                return None

            # 可以在这里添加更多响应处理逻辑
        else:
            print(f"[{time.strftime('%H:%M:%S')}] 未收到响应")
            return None

    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] 发送或接收时发生错误: {e}")
        return None


def build_message(json_data):
    asdu_data = json_data.encode('utf-8')
    data_length = len(asdu_data)
    header = build_protocol_header(data_length=data_length, msg_id=1, asdu_format=0x01)
    message = header + asdu_data
    return message

def record_point(client_sock):
    json_dict = \
    {
        "PatrolDevice": {
            "Type": 100,
            "Command": 100,
            "Time": "2023-01-01 00:00:00",
            "Items": {
            }
        }
    }
    json_str = json.dumps(json_dict)
    message = build_message(json_str)
    json_obj = send_and_receive_thread(client_sock, message)
    if message_filtter(json_obj, 1002, 5) is True:
            print("当前经度是",json_obj["PatrolDevice"]["Items"]["GPS"]["Latitude"])

def message_filtter(json_obj, type, command) -> bool:
    if json_obj is None: 
        return False
    if ('Type' not in json_obj['PatrolDevice']) or ('Command' not in json_obj['PatrolDevice']):
        return False
    if json_obj['PatrolDevice']['Command']!= command or json_obj['PatrolDevice']['Type']!= type:
        return False
    return True
 

def main():
    client_sock = None

    try:
        # 创建socket并连接
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((SERVER_IP, SERVER_PORT))
        print(f"已连接到机器狗，ip为： {SERVER_IP}:{SERVER_PORT}")


        while True:
            if record_point(client_sock):
                print("保存点位成功")
            else:
                print("保存点位失败")
                break
            time.sleep(0.5)

    except Exception as e:
        print(f"程序发生错误: {e}")
    finally:
        # 关闭socket
        if client_sock:
            client_sock.close()
            print("连接已关闭")


if __name__ == "__main__":
    main()