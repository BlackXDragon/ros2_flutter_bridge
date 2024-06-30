import rclpy
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor

from rclpy.action import ActionServer, ActionClient

import websockets
import asyncio
import threading

import json

from functools import partial

from traceback import print_exc, format_exc

ROS2NativeTypes = [
    'bool',
    'byte',
    'char',
    'float32',
    'float64',
    'int8',
    'uint8',
    'int16',
    'uint16',
    'int32',
    'uint32',
    'int64',
    'uint64',
    'string',
    'wstring',
]

class Ros2FlutterBridge(Node):
    
    # Topics
    pubs = {}
    subs = {}
    
    # Actions
    action_servers = {} # Not implemented yet
    received_goals = {} # Not implemented yet
    action_clients = {}
    sent_goals = {}
    
    ws_server = None
    
    def __init__(self):
        super().__init__('ros2_flutter_bridge')
        
        # Create a websocket server
        self.ws_server = websockets.serve(self.on_connect, "0.0.0.0", 9999)
        self.ws = None
        self.exit = False
        self.client_disconnected = False
        
        self.get_logger().info("ROS2 Flutter Bridge started")
        
    def get_msg_type(self, msgType):
        pkg = '.'.join(msgType[:-1])
        cls = msgType[-1]
        module = __import__(pkg, fromlist=[cls])
        return getattr(module, cls)
    
    def ros2_msg_to_dict(self, msg):
        msg_data = {}
        msg_data['fields'] = []
        for field in msg.get_fields_and_field_types().items():
            f = {}
            f['name'] = field[0]
            f['type'] = field[1]
            f['value'] = getattr(msg, field[0])
            if f['type'] not in ROS2NativeTypes:
                if f['type'].endswith(']'):
                    f['type'] = f['type'].split('[')[0] + '[]'
                    f['value'] = list(f['value'])
                elif f['type'].startswith('sequence'):
                    f['type'] = f['type'].split('<')[1].split('>')[0] + '[]'
                    f['value'] = list(f['value'])
                else:
                    f['value'] = self.ros2_msg_to_dict(f['value'])
            msg_data['fields'].append(f)
        msg_data['name'] = msg.__class__.__name__
        return msg_data
    
    def dict_to_ros2_msg(self, msg_data, msgType=None):
        if not msgType:
            msgType = msg_data['name'].split('/')
            msgType = self.get_msg_type(msgType)
        msg = msgType()
        for field in msg_data['fields']:
            if not any([field['type'].startswith(t) for t in ROS2NativeTypes]):
                setattr(msg, field['name'], self.dict_to_ros2_msg(field['value']))
            else:
                setattr(msg, field['name'], field['value'])
        return msg
        
    async def on_connect(self, websocket, path):
        if self.ws:
            try:
                await self.ws.send('{"op": "error", "message": "Another client is trying to connect"}')
                print("Another client is already connected")
                await websocket.send(json.dumps({'op': 'error', 'message': 'Another client is already connected'}))
                await websocket.close()
                return
            except websockets.exceptions.ConnectionClosedError:
                self.client_disconnected = True
                while self.ws and self.client_disconnected:
                    await asyncio.sleep(1)
            
        print("Connected")
        self.ws = websocket
        while True:
            if self.exit or self.client_disconnected:
                raise websockets.exceptions.ConnectionClosedError
            try:
                message = await self.ws.recv()
                data = json.loads(message)
                if data['op'] == 'create_publisher':
                    if data['topic'] not in self.pubs:
                        msgType = data['message_type']['name'].split('/')
                        msgType = self.get_msg_type(msgType)
                        t = self.create_publisher(msgType, data['topic'], 10)
                        self.pubs[data['topic']] = t
                    else:
                        await self.ws.send(json.dumps({'op': 'error', 'message': 'Publisher already exists'}))
                elif data['op'] == 'create_subscription':
                    if data['topic'] not in self.subs:
                        msgType = data['message_type']['name'].split('/')
                        msgType = self.get_msg_type(msgType)
                        t = self.create_subscription(msgType, data['topic'], partial(self.on_msg_received, data['topic'], data['message_type']['name']), 10)
                        self.subs[data['topic']] = t
                    else:
                        await self.ws.send(json.dumps({'op': 'error', 'message': 'Subscription already exists'}))
                elif data['op'] == 'publish':
                    if data['topic'] in self.pubs:
                        msg = self.dict_to_ros2_msg(data['msg'])
                        self.pubs[data['topic']].publish(msg)
                    else:
                        await self.ws.send(json.dumps({'op': 'error', 'message': 'Publisher does not exist'}))
                elif data['op'] == 'create_action_client':
                    if data['action_server'] not in self.action_clients:
                        actionType = data['action_type']['name'].split('/')
                        actionType = self.get_msg_type(actionType)
                        t = ActionClient(self, actionType, data['action_server'])
                        self.action_clients[data['action_server']] = (t, actionType, data['action_type'])
                    else:
                        await self.ws.send(json.dumps({'op': 'error', 'message': 'Action client already exists'}))
                elif data['op'] == 'send_goal':
                    if data['action_server'] in self.action_clients:
                        client, actionType, actionTypeStruct = self.action_clients[data['action_server']]
                        goalType = actionType.Goal
                        goal = self.dict_to_ros2_msg(data['goal'], goalType)
                        try:
                            client.wait_for_server(timeout_sec=1)
                        except Exception as e:
                            await self.ws.send(json.dumps({'op': 'error', 'message': 'Action server not available'}))
                        try:
                            future = client.send_goal_async(
                                goal,
                                feedback_callback = partial(self.on_action_feedback, data['action_server'], data['tempGoalID'], actionTypeStruct))
                            future.add_done_callback(partial(self.on_goal_response, data['action_server'], data['tempGoalID'], actionTypeStruct))
                            if data['action_server'] not in self.sent_goals:
                                self.sent_goals[data['action_server']] = {}
                            self.sent_goals[data['action_server']][data['tempGoalID']] = {'goalID': None, 'handle': None, 'cancelled': False}
                        except Exception as e:
                            await self.ws.send(json.dumps({'op': 'error', 'message': str(e), 'traceback': format_exc()}))
                    else:
                        await self.ws.send(json.dumps({'op': 'error', 'message': 'Action client does not exist'}))
                elif data['op'] == 'cancel_goal':
                    if data['action_server'] in self.action_clients:
                        goals = self.sent_goals[data['action_server']]
                        goal_handle = None
                        if data['goalID'] in goals:
                            goal = goals[data['goalID']]
                            goal_handle = goal['handle']
                        elif data['goalID'] in [g['goalID'] for g in goals.values()]:
                            goal = [g for g in goals.values() if g['goalID'] == data['goalID']][0]
                            goal_handle = goal['handle']
                        else:
                            await self.ws.send(json.dumps({'op': 'error', 'message': 'Goal not found'}))
                        if goal_handle:
                            goal['cancelled'] = True
                            res = goal_handle.cancel_goal()
                            if res.return_code != 0:
                                goal['cancelled'] = False
                            await self.ws.send(json.dumps({
                                'op': 'action_cancel_response',
                                'goalID': data['goalID'],
                                'action_server': data['action_server'],
                                'success': res.return_code == 0
                                }))
                        else:
                            await self.ws.send(json.dumps({'op': 'error', 'message': 'Goal exists but has not been accepted yet'}))
                    else:
                        await self.ws.send(json.dumps({'op': 'error', 'message': 'Action client does not exist'}))
                else:
                    self.get_logger().info(f"Unknown operation: {data['op']}")
                    
            except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK):
                print("Connection closed")
                self.ws = None
                self.cleanupROSInterfaces()
                if self.client_disconnected:
                    self.client_disconnected = False
                break
                    
            except Exception as e:
                print(f"Error: {e}")
                print_exc()
                await self.ws.send(json.dumps({'op': 'error', 'message': str(e), 'traceback': format_exc()}))
        
    async def on_msg_received(self, topic_name, msg_name, msg):
        msg_data = self.ros2_msg_to_dict(msg)
        msg_data['name'] = msg_name
        data = {
            'op': 'subscribe',
            'topic': topic_name,
            'msg': msg_data
        }
        if self.ws:
            await self.ws.send(json.dumps(data))
            
    async def on_action_feedback(self, action_server, goalID, actionTypeStruct, feedback):
        if self.sent_goals[action_server][goalID]['cancelled']:
            return
        
        feedback_data = self.ros2_msg_to_dict(feedback.feedback)
        feedback_data['name'] = actionTypeStruct['feedback']['name']
        
        gid = self.sent_goals[action_server][goalID]['goalID']
        if gid is None:
            gid = goalID
        
        data = {
            'op': 'action_feedback',
            'goalID': gid,
            'action_server': action_server,
            'feedback': feedback_data
        }
        if self.ws:
            await self.ws.send(json.dumps(data))
            
    async def on_goal_response(self, action_server, goalID, actionTypeStruct, future):
        goal_handle = future.result()
        uuid = goal_handle.goal_id.uuid
        uuid_str = ''.join(['%02x' % i for i in uuid])
        self.sent_goals[action_server][goalID]['goalID'] = uuid_str
        self.sent_goals[action_server][goalID]['handle'] = goal_handle
        data = {
            'op': 'update_goal_id',
            'tempGoalID': goalID,
            'goalID': uuid_str,
            'action_server': action_server
        }
        if self.ws:
            await self.ws.send(json.dumps(data))
        data = {
            'op': 'goal_response',
            'goalID': uuid_str,
            'action_server': action_server,
            'accepted': goal_handle.accepted
        }
        if self.ws:
            await self.ws.send(json.dumps(data))
        future = goal_handle.get_result_async()
        future.add_done_callback(partial(self.on_goal_result, action_server, goalID, actionTypeStruct))
        
    async def on_goal_result(self, action_server, goalID, actionTypeStruct, future):
        if self.sent_goals[action_server][goalID]['cancelled']:
            return
        
        result = future.result()
        result_data = self.ros2_msg_to_dict(result.result)
        result_data['name'] = actionTypeStruct['result']['name']
        data = {
            'op': 'action_result',
            'goalID': self.sent_goals[action_server][goalID]['goalID'],
            'action_server': action_server,
            'result': result_data
        }
        if self.ws:
            await self.ws.send(json.dumps(data))
        
    def cleanupROSInterfaces(self):
        # Topics
        for topic in self.pubs:
            self.destroy_publisher(self.pubs[topic])
        for topic in self.subs:
            self.destroy_subscription(self.subs[topic])
        self.pubs = {}
        self.subs = {}
        
        # Actions
        for action in self.action_clients:
            self.action_clients[action][0].destroy()
        self.action_clients = {}
        self.sent_goals = {}
            
    def destroy_node(self):
        self.exit = True
        if self.ws:
            self.ws.close()
            self.cleanupROSInterfaces()
        super().destroy_node()

def main():
    rclpy.init()
    node = Ros2FlutterBridge()
    executor = MultiThreadedExecutor()
    
    t = threading.Thread(target=rclpy.spin, args=(node, executor))
    t.start()
    try:
        asyncio.get_event_loop().run_until_complete(node.ws_server)
        asyncio.get_event_loop().run_forever()
    finally:
        t.join()
        node.destroy_node()
        executor.shutdown()

if __name__ == '__main__':
    main()
