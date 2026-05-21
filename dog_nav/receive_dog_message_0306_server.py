import datetime
import os
import pickle
import socket
import time
import threading
import struct
import json
from queue import Queue, Empty


SERVER_IP = '10.21.31.103'
SERVER_PORT = 30001

# 全局变量
message_queue = Queue()  # 消息队列，用于接收线程和主线程通信
stop_receiver = threading.Event()  # 停止接收线程的标志
pending_requests = {}  # 等待响应的请求字典: {msg_id: threading.Event}
request_responses = {}  # 存储响应结果: {msg_id: response}
msg_id_counter = 0  # 消息ID计数器
msg_id_lock = threading.Lock()  # 保护msg_id_counter的锁
pending_requests_lock = threading.Lock()  # 保护pending_requests的锁
request_responses_lock = threading.Lock()  # 保护request_responses的锁


def get_next_msg_id():
    """获取下一个消息ID"""
    global msg_id_counter
    with msg_id_lock:
        msg_id_counter = (msg_id_counter + 1) % 65536
        if msg_id_counter == 0:
            msg_id_counter = 1
        return msg_id_counter


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
    """接收并解析单个响应数据包"""
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
        return None
    except Exception as e:
        print(f"接收响应时发生错误: {e}")
        return None


def receiver_thread(sock):
    """专门的接收线程，持续接收所有消息并放入队列"""
    print("[接收线程] 启动...")
    
    while not stop_receiver.is_set():
        try:
            response = receive_response(sock, timeout=0.5)
            if response and "json_obj" in response:
                json_obj = response['json_obj']
                msg_id = response['header']['msg_id']
                
                # 检查是否有请求在等待这个响应
                event = None
                with pending_requests_lock:
                    if msg_id in pending_requests:
                        event = pending_requests.get(msg_id)
                
                if event:
                    # 这是某个请求的响应，存储结果并通知等待线程
                    with request_responses_lock:
                        request_responses[msg_id] = response
                    event.set()
                else:
                    # 这是心跳或其他主动推送的消息，放入队列
                    message_queue.put(json_obj)
                    
        except Exception as e:
            if not stop_receiver.is_set():
                print(f"[接收线程] 错误: {e}")
    
    print("[接收线程] 已停止")


def send_and_wait_response(sock, message, expected_msg_id, timeout=10):
    """发送消息并等待特定msg_id的响应"""
    # 创建事件用于等待响应
    event = threading.Event()
    
    # 注册等待
    with pending_requests_lock:
        pending_requests[expected_msg_id] = event
    
    try:
        # 发送数据
        sock.sendall(message)
        print(f"[{time.strftime('%H:%M:%S')}] 发送成功，等待msg_id={expected_msg_id}的响应...")
        
        # 等待响应
        if event.wait(timeout=timeout):
            # 收到响应
            with request_responses_lock:
                response = request_responses.pop(expected_msg_id, None)
            with pending_requests_lock:
                pending_requests.pop(expected_msg_id, None)
            
            if response and 'json_obj' in response:
                return response['json_obj']
            return response
        else:
            # 超时
            print(f"[{time.strftime('%H:%M:%S')}] 等待响应超时")
            with pending_requests_lock:
                pending_requests.pop(expected_msg_id, None)
            return None
            
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] 发送错误: {e}")
        with pending_requests_lock:
            pending_requests.pop(expected_msg_id, None)
        return None


def build_message(json_data, msg_id=None):
    """构建消息"""
    if msg_id is None:
        msg_id = get_next_msg_id()
    asdu_data = json_data.encode('utf-8')
    data_length = len(asdu_data)
    header = build_protocol_header(data_length=data_length, msg_id=msg_id, asdu_format=0x01)
    message = header + asdu_data
    return message, msg_id


def record_point(client_sock, point_list):
    """记录当前点位"""
    json_dict = {
        "PatrolDevice": {
            "Type": 1007,
            "Command": 2,
            "Time": f"{datetime.datetime.now()}",
            "Items": {}
        }
    }
    json_str = json.dumps(json_dict)
    message, msg_id = build_message(json_str)
    
    # 发送并等待响应
    json_obj = send_and_wait_response(client_sock, message, msg_id, timeout=5)
    
    if json_obj is None:
        print("获取点位信息失败：未收到响应")
        return 0
    
    # 检查是否是期望的响应类型
    if message_filtter(json_obj, 1007, 2) is False:
        print(f"收到非期望响应: Type={json_obj.get('PatrolDevice', {}).get('Type')}, Command={json_obj.get('PatrolDevice', {}).get('Command')}")
        return 0
    
    items = json_obj.get("PatrolDevice", {}).get("Items", {})
    
    if items.get("Location") == 1:
        print("定位丢失，请重新尝试或者根据手册流程进行定位初始化")
        return 0
    else:
        point_list.append([
            items.get("PosX"),
            items.get("PosY"),
            items.get("PosZ"),
            items.get("Yaw")
        ])
        return 1


def navigate_point(client_sock, point_list):
    """导航到点位列表"""
    charge_mode = input("导航结束后是否需要自主充电，需要充电则按1，否则按回车（请注意，最后一个点会设置为充电点）：")
    
    for i, point in enumerate(point_list):
        point_tag = 0
        if charge_mode == "1" and i == len(point_list) - 1:
            point_tag = 3
            
        json_dict = {
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
                    "PointInfo": point_tag,
                    "Gait": int('3002', 16),
                    "Speed": 0,
                    "Manner": 0,
                    "ObsMode": 0,
                    "NavMode": 1
                }
            }
        }
        json_str = json.dumps(json_dict)
        message, msg_id = build_message(json_str)
        
        # 发送并等待响应，最多重试3次
        for attempt in range(3):
            response = send_and_wait_response(client_sock, message, msg_id, timeout=10)
            
            if response and message_filtter(response, 1003, 1):
                error_code = response.get('PatrolDevice', {}).get('Items', {}).get('ErrorCode')
                if error_code == 8960:
                    print(f"单点 {i+1}/{len(point_list)} 下发成功")
                    break
                elif error_code == 57352:
                    print(f"单点 {i+1} 正在处理中，等待...")
                    time.sleep(1)
                    continue
            
            if attempt < 2:
                print(f"单点 {i+1} 响应异常，重试...")
                time.sleep(1)
        else:
            print(f"单点 {i+1} 下发失败，跳过")
            
        time.sleep(1)

    print("导航结束")


def enter_charge(client_sock):
    """进入充电"""
    json_dict = {
        "PatrolDevice": {
            "Type": 2,
            "Command": 24,
            "Time": "2023-01-01 00:00:00",
            "Items": {
                "Charge": 1
            }
        }
    }
    json_str = json.dumps(json_dict)
    message, msg_id = build_message(json_str)
    json_obj = send_and_wait_response(client_sock, message, msg_id, timeout=5)
    
    if json_obj is None:
        print("进入充电失败, 未获得机器狗响应，请检查通讯")
        return 0
    return 1


def quit_charge(client_sock):
    """退出充电"""
    json_dict = {
        "PatrolDevice": {
            "Type": 2,
            "Command": 24,
            "Time": "2023-01-01 00:00:00",
            "Items": {
                "Charge": 0
            }
        }
    }
    json_str = json.dumps(json_dict)
    message, msg_id = build_message(json_str)
    json_obj = send_and_wait_response(client_sock, message, msg_id, timeout=5)
    
    if json_obj is None:
        print("退出充电失败, 未获得机器狗响应，请检查通讯")
        return 0
    return 1


def stand_up(client_sock):
    """起立"""
    json_dict = {
        "PatrolDevice": {
            "Type": 2,
            "Command": 22,
            "Time": "2023-01-01 00:00:00",
            "Items": {
                "MotionParam": 1
            }
        }
    }
    json_str = json.dumps(json_dict)
    message, msg_id = build_message(json_str)
    json_obj = send_and_wait_response(client_sock, message, msg_id, timeout=5)
    
    if json_obj is None:
        print("起立失败, 未获得机器狗响应，请检查通讯")
        return 0
    return 1


def wake_up(client_sock):
    """唤醒"""
    json_dict = {
        "PatrolDevice": {
            "Type": 1101,
            "Command": 6,
            "Time": "2023-01-01 00:00:00",
            "Items": {
                "Sleep": False,
                "Auto": True,
                "Time": 5
            }
        }
    }
    json_str = json.dumps(json_dict)
    message, msg_id = build_message(json_str)
    json_obj = send_and_wait_response(client_sock, message, msg_id, timeout=5)
    
    if json_obj is None:
        print("唤醒失败, 未获得机器狗响应，请检查通讯")
        return 0
    return 1


def message_filtter(json_obj, type_val, command_val) -> bool:
    """过滤消息类型"""
    if json_obj is None:
        return False
    
    patrol_device = json_obj.get('PatrolDevice', {})
    if patrol_device.get('Type') != type_val:
        return False
    if patrol_device.get('Command') != command_val:
        return False
    return True


def initiate_navigate(client_sock):
    """初始化导航"""
    json_dict = {
        "PatrolDevice": {
            "Type": 100,
            "Command": 100,
            "Time": "2023-01-01 00:00:00",
            "Items": {}
        }
    }
    json_str = json.dumps(json_dict)
    message, msg_id = build_message(json_str)
    
    # 等待正确的初始化响应
    for _ in range(20):  # 最多等待10秒
        json_obj = send_and_wait_response(client_sock, message, msg_id, timeout=5)
        
        if json_obj is None:
            print("初始化失败, 未获得机器狗响应，请检查通讯")
            return 0
            
        items = json_obj.get("PatrolDevice", {}).get("Items", {})
        
        if "ErrorCode" not in items and json_obj.get("PatrolDevice", {}).get("Command") == 6:
            break
            
        time.sleep(0.5)
    else:
        print("初始化超时")
        return 0

    # 检查状态并执行相应操作
    basic_status = json_obj.get("PatrolDevice", {}).get("Items", {}).get("BasicStatus", {})
    
    if basic_status.get("Sleep") == 1:
        print("机器狗处于睡眠状态，正在唤醒...")
        wake_up(client_sock)
        time.sleep(10)
        
    if basic_status.get("Charge") == 2:
        print("机器狗正在充电，正在退出充电...")
        quit_charge(client_sock)
        time.sleep(10)
        
    if basic_status.get("MotionState") == 4:
        print("机器狗趴下状态，正在起立...")
        stand_up(client_sock)
        time.sleep(10)
        
    return 1


def main():
    client_sock = None
    receiver = None

    try:
        # 创建socket并连接
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((SERVER_IP, SERVER_PORT))
        print(f"已连接到机器狗，ip为： {SERVER_IP}:{SERVER_PORT}")

        # 启动接收线程
        receiver = threading.Thread(target=receiver_thread, args=(client_sock,), daemon=True)
        receiver.start()

        while True:
            mode_num = input("请选择使用模式，打点是1，导航是2：")
            
            if mode_num not in ['1', '2']:
                print("无效输入，请重新选择")
                continue
                
            # 初始化
            if not initiate_navigate(client_sock):
                print("初始化失败，请检查连接")
                continue

            if mode_num == '1':
                # 打点模式
                point_list = []
                print("现在进入打点模式, 打点请直接按回车，退出按q再按回车：")
                
                while True:
                    user_input = input()
                    
                    if user_input == 'q':
                        # 保存点位
                        nav_name = input(f"保存了{len(point_list)}个点位，请输入导航名称，如果按回车则使用默认名称：")
                        if nav_name == '':
                            nav_name = 'data'
                        try:
                            with open(f'{nav_name}.pkl', 'wb') as f:
                                pickle.dump(point_list, f)
                            print(f"保存导航记录成功，名称为{nav_name}.pkl")
                        except Exception as e:
                            print(f"保存失败: {e}")
                        break
                    
                    # 记录点位
                    if record_point(client_sock, point_list):
                        print(f"保存点位成功，当前共{len(point_list)}个点位")
                    else:
                        print("保存点位失败")

            else:
                # 导航模式
                nav_name = input("请输入想要读取的导航记录（直接回车使用默认名称data）：")
                if nav_name == '':
                    nav_name = 'data'
                    
                try:
                    with open(f'{nav_name}.pkl', 'rb') as file:
                        point_list = pickle.load(file)
                    print(f"成功加载导航记录，共{len(point_list)}个点位")
                    navigate_point(client_sock, point_list)
                except FileNotFoundError:
                    print("没找到打点数据文件，请重新生成")
                except PermissionError:
                    print("权限拒绝，可能是需要给程序管理员权限")
                except Exception as e:
                    print(f"加载导航记录失败: {e}")

    except KeyboardInterrupt:
        print("\n用户中断程序")
    except Exception as e:
        print(f"程序发生错误: {e}")
    finally:
        # 停止接收线程
        stop_receiver.set()
        
        # 关闭socket
        if client_sock:
            client_sock.close()
            print("连接已关闭")
            
        # 等待接收线程结束
        if receiver and receiver.is_alive():
            receiver.join(timeout=2)


if __name__ == "__main__":
    main()
