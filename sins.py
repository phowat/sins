__author__ = 'github.com/phowat'

import tornado.web
from tornado.websocket import WebSocketHandler
from tornado.ioloop import IOLoop
import Queue
from threading import Thread
import json
from itertools import cycle

sessions = {}
queues = {}
pairs = {}

class SSession(object):
    def __init__(self, write_message):
        self.topics = []
        self.write_message = write_message
        print "Created Session."

class SQueue(object):
    def __init__(self):
        self.listeners = []
        self.messages = Queue.Queue()
        self.current = None
        self.cycle = None

class SPairSide(object):
    def __init__(self):
        self.key = None
        self.messages = Queue.Queue()

class SPair(object):
    def __init__(self):
        self.a_side = SPairSide()
        self.b_side = SPairSide()
        
class SinsHandler(WebSocketHandler):

    def __get_request_key(self):
        return self.request.headers['Sec-Websocket-Key'].strip('=')

    def __prepare_queue(self, name):
        try:
            queue = queues[name]
        except:
            queues[name] = SQueue()
            queue = queues[name]
        return queue

    def open(self):
        self.key = self.__get_request_key()
        print "Opening connection %s" % (self.key)
        sessions[self.key] = SSession(self.write_message)

    def __on_subscribe(self, dest_type, dest_name):
        if dest_type == 'topic':
            session = sessions[self.key]
            if dest_name not in session.topics:
                session.topics.append(dest_name)

        elif dest_type == 'queue':

            queue = self.__prepare_queue(dest_name)
            if self.key not in queue.listeners:
                queue.listeners.append(self.key)

            queue.cycle = cycle(queue.listeners)
            if queue.current is not None:
                while queue.current != queue.cycle.next():
                    continue

            if not queue.messages.empty():
               while True:
                    try:
                        msg = queue.messages.get_nowait()
                    except Queue.Empty:
                        break
                    self.write_message(msg)
        elif dest_type == 'pair':
            try:
                pair = pairs[dest_name]
                side = "a"
            except:
                pairs[dest_name] = SPair()
                pair = pairs[dest_name]
                side = "b"

            if side == "a":
                side = pair.a_side
            elif pair.b_side.key is None:
                side = pair.b_side
            else:
                print "Connection already paired."
                # TODO: Inform failure to client.
            side.key = self.key
            if not side.messages.empty():
               while True:
                    try:
                        msg = side.messages.get_nowait()
                    except Queue.Empty:
                        break
                    self.write_message(msg)
        else:
            print "Unknown destination type "+dest_type +"."

    def __on_unsubscribe(self, dest_type, dest_name):
        if dest_type == 'topic':
            session = sessions[self.key]
            if dest_name not in session.topics:
                session.topics.remove(dest_name)

        elif dest_type == 'queue':
            try:
                queue = queues[dest_name]
            except:
                print "Unknown destination "+dest_name+"."
                return

            queue.listeners.remove(self.key)
            cur_listener = queue.current
            if cur_listener == self.key:
                cur_listener = queue.cycle.next()
            queue.cycle = cycle(queue.listeners)

            if len(queue.listeners) < 1 and queue.messages.empty():
                del(queues[dest_name])
            elif cur_listener is not None:
                while cur_listener != queue.cycle.next():
                    continue

        elif dest_type == 'pair':
            #TODO: Refactor
            try:
                pair = pairs[dest_name]
            except:
                print "Unknown destination "+dest_name+"."
                return

            if pair.a_side.key == self.key:
                sessions[pair.b_side.key].write_message(
                    json.dumps({"command": "DISCONNECT"}))
            elif pair.a_side.key == self.key:
                sessions[pair.a_side.key].write_message(
                    json.dumps({"command": "DISCONNECT"}))
            else:
                print "Unsubscribe from party not subscribed."
                return

            del(pairs[dest_name])

        else:
            print "Unknown destination type "+dest_type +"."

    def __on_send(self, dest_type, dest_name, content):
        print "Send message [%s] to destination /%s/%s" % \
              (content, dest_type, dest_name)
        if dest_type == 'topic':
            for sname in sessions:
                if dest_name in sessions[sname].topics:
                    sessions[sname].write_message(content)

        elif dest_type == 'queue':
            queue = self.__prepare_queue(dest_name)
            if len(queues[dest_name].listeners) > 0:
                current = queue.cycle.next()
                sessions[current].write_message(content)
                queue.current = current
            else:
                queue.messages.put_nowait(content)

        elif dest_type == 'pair':
            #TODO: Refactor
            #TODO: Pairs should support receiveing messages while the other
            # side hasn't connected

            try:
                pair = pairs[dest_name]
            except:
                print "Unknown destination "+dest_name+"."
                return

            if pair.a_side.key == self.key:
                side = pair.b_side
            elif pair.b_side.key == self.key:
                side = pair.a_side
            else:
                print "Message from endpoint not in this pair."
                return

            if side.key is not None:
                sessions[side.key].write_message(content)
            else:
                side.messages.put_nowait(content)
                

    def on_message(self, message):
        info = None
        try:
            info = json.loads(message)
            command = info['command']
        except:
            print "Malformed JSON object received."
            return

        print "Got Message: [%s]." % (message)

        (__slash, dest_type, dest_name) = info['destination'].split('/')
        if command == 'SEND':
            self.__on_send(dest_type, dest_name, info['content'])

        elif command == 'SUBSCRIBE':
            self.__on_subscribe(dest_type, dest_name)

        elif command == 'UNSUBSCRIBE':
            self.__on_unsubscribe(dest_type, dest_name)

        elif command == 'DISCONNECT':
            print 'DISCONNECT'
        else:
            print "Unknown command "+str(command)

class StatsHandler(tornado.web.RequestHandler):
    def get(self):
        stats = 'STATS:<br>'

        for qname in queues:
            stats += "Queue: %s <br> Current: %s <br> Listeners: %s<br>" % \
                     (qname, queues[qname].current, str(queues[qname].listeners))

        for sname in sessions:
            stats += "Session id: %s <br> Topics: %s <br>" %\
                     (sname, str(sessions[sname].topics))

        self.write(stats)

if __name__ == '__main__':
    application=tornado.web.Application([
        (r'/sins',SinsHandler),
        (r'/stats',StatsHandler),
    ])
    application.listen(5000)
    IOLoop.instance().start()
