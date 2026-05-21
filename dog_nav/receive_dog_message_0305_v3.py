# import socket
# import time
# import threading
#
# def build_protocol_header(data_length: int, msg_id: int = 1, asdu_format: int = 0x01) -> bytearray:
#     if not (0 <= data_length <= 65535):
#         raise ValueError('data_length must be between 0 and 65535')
#     if not (0 <= msg_id <= 65535):
#         raise ValueError('msg_id must be between 0 and 65535')
#     if asdu_format not in [0x01, 0x00]:
#         raise ValueError('asdu_format must be 0x01 or 0x00')
#
#     header = bytearray(16)
#     header[0] = 0xeb
#     header[1] = 0x91
#     header[2] = 0xeb
#     header[3] = 0x90
#     header[4] = data_length & 0xFF
#     header[5] = (data_length >> 8) & 0xFF
#     header[6] = msg_id & 0xFF
#     header[7] = (msg_id >> 8) & 0xFF
#     header[8] = asdu_format
#     return header
#
# SERVER_IP = '10.21.31.103'
# SERVER_PORT = 30001
#
# json_data = """
# {
# 	"PatrolDevice": {
# 		"Type": 100,
# 		"Command": 100,
# 		"Time": "2023-01-01 00:00:00",
# 		"Items": {
# 		}
# 	}
# }
# """
#
# asdu_data = json_data.encode('utf-8')
# data_length = len(asdu_data)
#
# header = build_protocol_header(data_length = data_length, msg_id = 1, asdu_format = 0x01)
# message = header + asdu_data
# try:
#     while True:
#         client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         client_sock.connect((SERVER_IP, SERVER_PORT))
#         send_len = client_sock.sendall(message)
#         print("发送成功")
#         time.sleep(0.01)
# except Exception as e:
#     print(e)
# finally:
#     client_sock.close()
import datetime
import os
import pickle
import socket
import time
import threading
import struct
import json



SERVER_IP = '10.21.31.103'
SERVER_PORT = 30001

def message_filtter(json_obj, type, command) -> bool:
    if json_obj is None: 
        return False
    if ('type' not in json_obj) or ('command' not in json_obj): 
        return False
    if json_obj['PatrolDevice']['command']!= command or json_obj['PatrolDevice']['Type']!= type:
        return False
    return True


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


def record_point(client_sock, point_list):
    json_dict = \
        {
        "PatrolDevice": {
            "Type": 1007,
            "Command": 2,
            "Time": f"{datetime.datetime.now()}",
            "Items": {}
        }
    }
    json_str = json.dumps(json_dict)
    message = build_message(json_str)
    json_obj = send_and_receive_thread(client_sock, message)
    if message_filtter(json_obj, 1007, "Items") is False:
        time.sleep(0.5)
    else:
        if json_obj["PatrolDevice"]["Items"]["Location"] == 1:
            print("定位丢失，请重新尝试或者根据手册流程进行定位初始化")
            return 0
        else:
            point_list.append([json_obj["PatrolDevice"]["Items"]["PosX"], json_obj["PatrolDevice"]["Items"]["PosY"], json_obj["PatrolDevice"]["Items"]["PosZ"], json_obj["PatrolDevice"]["Items"]["Yaw"]])
            return 1

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

def navigate_point(client_sock, point_list):
    for point in point_list:
        json_dict = \
        {
            "PatrolDevice": {
                "Type": 1003,
                "Command": 1,
                "Time": f"{datetime.datetime.now()}",
                "Items": {
                     "Value": 0,
                     "MapID": 0,
                     "PosX": point[0],
                     "PosY": point[1],
                     "PosZ": point[2],
                     "AngleYaw": point[3],
                     "PointInfo": 0,
                     "Gait": int('3002', 16),
                     "Speed": 0,
                     "Manner": 0,
                     "ObsMode": 0,
                     "NavMode": 1
                }
            }
        }
        json_str = json.dumps(json_dict)
        message = build_message(json_str)
        # response = send_and_receive_thread(client_sock, message)
        while True:
            response = send_and_receive_thread(client_sock, message)
            if response is not None:
                if response['PatrolDevice']['Items']['ErrorCode'] == 8960:
                    print("单点下发成功")
                    break
                elif response['PatrolDevice']['Items']['ErrorCode'] == 57352:
                    time.sleep(1)


        # while True:
        #     if response:
        #         if response['PatrolDevice']['Items']['ErrorCode'] != 8960:
        #             time.sleep(1/20)
        #         else:
        #             break
        #     else:
        #         response = receive_response(client_sock, timeout=5)

        time.sleep(1)
    print("导航结束")

def build_message(json_data):
    asdu_data = json_data.encode('utf-8')
    data_length = len(asdu_data)
    header = build_protocol_header(data_length=data_length, msg_id=1, asdu_format=0x01)
    message = header + asdu_data
    return message


def main():
    client_sock = None

    try:
        # 创建socket并连接
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((SERVER_IP, SERVER_PORT))
        print(f"已连接到机器狗，ip为： {SERVER_IP}:{SERVER_PORT}")


        while True:
            mode_num = input("请选择使用模式，打点是1，导航是2：")
            if mode_num == '1':
                point_list = []
                while True:
                    user_input = input("现在进入打点模式, 打点请直接按回车，退出按q再按回车：")
                    if user_input == 'q':
                        nav_name = input(f"保存了{len(point_list)}个点位，请输入导航名称，如果按回车则使用默认名称：")
                        if nav_name == '':
                            nav_name = 'data'
                        with open(f'{nav_name}.pkl', 'wb') as f:
                            pickle.dump(point_list, f)
                        print(f"保存导航记录成功，名称为{nav_name}.pkl")
                        break
                    if record_point(client_sock, point_list):
                        print("保存点位成功")
                    else:
                        print("保存点位失败")
                        break

            else:
                try:
                    nav_name = input("请输入想要读取的导航记录：")
                    with open(f'{nav_name}.pkl', 'rb') as file:
                        point_list = pickle.load(file)
                except FileNotFoundError:
                    print("没找到打点数据文件，请重新生成或是联系小樊")
                except PermissionError:
                    print("权限拒绝，可能是需要给程序管理员权限")
                navigate_point(client_sock, point_list)







    except Exception as e:
        print(f"程序发生错误: {e}")
    finally:
        # 关闭socket
        if client_sock:
            client_sock.close()
            print("连接已关闭")


if __name__ == "__main__":
    main()