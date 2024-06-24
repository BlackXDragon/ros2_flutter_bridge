import rclpy
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor

import websockets
import asyncio
import threading

import json

from functools import partial

from traceback import print_exc, format_exc

class Ros2FlutterBridge(Node):
    
    # Topics
    pubs = {}
    subs = {}
    
    ws_server = None
    
    def __init__(self):
        super().__init__('ros2_flutter_bridge')
        
        # Create a websocket server
        self.ws_server = websockets.serve(self.on_connect, "0.0.0.0", 9999)
        self.ws = None
        
        self.get_logger().info("ROS2 Flutter Bridge started")
        
    def get_msg_type(self, msgType):
        pkg = '.'.join(msgType[:-1])
        cls = msgType[-1]
        module = __import__(pkg, fromlist=[cls])
        return getattr(module, cls)
        
    async def on_connect(self, websocket, path):
        print("Connected")
        self.ws = websocket
        try:
            async for message in websocket:
                try:
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
                            msg_data = {}
                            for field in data['msg']['fields']:
                                msg_data[field['name']] = field['value']
                            msgType = data['msg']['name'].split('/')
                            msgType = self.get_msg_type(msgType)
                            msg = msgType(**msg_data)
                            self.pubs[data['topic']].publish(msg)
                        else:
                            await self.ws.send(json.dumps({'op': 'error', 'message': 'Publisher does not exist'}))
                        
                except Exception as e:
                    print_exc()
                    await self.ws.send(json.dumps({'op': 'error', 'message': str(e), 'traceback': format_exc()}))
                
        except websockets.exceptions.ConnectionClosedError:
            print("Connection closed")
            self.ws = None
            self.cleanupROSInterfaces()
        
    async def on_msg_received(self, topic_name, msg_name, msg):
        msg_data = {}
        msg_data['fields'] = []
        for field in msg.get_fields_and_field_types().items():
            f = {}
            f['name'] = field[0]
            f['type'] = field[1]
            f['value'] = getattr(msg, field[0])
            msg_data['fields'].append(f)
        msg_data['name'] = msg_name
        data = {
            'op': 'subscribe',
            'topic': topic_name,
            'msg': msg_data
        }
        if self.ws:
            await self.ws.send(json.dumps(data))
        
    def cleanupROSInterfaces(self):
        for topic in self.pubs:
            self.pubs[topic].destroy()
        for topic in self.subs:
            self.subs[topic].destroy()
        self.pubs = {}
        self.subs = {}
            
    def destroy_node(self):
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
