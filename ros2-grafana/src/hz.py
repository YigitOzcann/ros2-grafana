from argparse import ArgumentTypeError
from collections import defaultdict
import functools
import math
import threading
import rclpy
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.clock import Clock, ClockType
from rclpy.qos import QoSProfile, QoSReliabilityPolicy, QoSHistoryPolicy
from ros2cli.node.direct import add_arguments as add_direct_node_arguments
from ros2cli.node.direct import DirectNode
from ros2topic.api import get_msg_class
from ros2topic.api import TopicNameCompleter
from ros2topic.verb import VerbExtension

DEFAULT_WINDOW_SIZE = 10000

def positive_int(string):
    try:
        value = int(string)
    except ValueError:
        value = -1
    if value <= 0:
        raise ArgumentTypeError('value must be a positive integer')
    return value

class HzVerb(VerbExtension):
    """Print the average publishing rate to screen."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            '--topic_list',
            help="Name of the ROS topic to listen to (e.g. '/chatter')"
        ).completer = TopicNameCompleter(include_hidden_topics_key='include_hidden_topics')
        
        parser.add_argument(
            '--window', '-w',
            dest='window_size', type=positive_int, default=DEFAULT_WINDOW_SIZE,
            help='Window size, in number of messages, for calculating rate (default: %d)' % DEFAULT_WINDOW_SIZE,
            metavar='WINDOW'
        )
        
        parser.add_argument(
            '--filter_expr',
            dest='filter_expr', default=None,
            help='Only measure messages matching the specified Python expression', 
            metavar='EXPR'
        )
        
        parser.add_argument(
            '--wall-time',
            dest='use_wtime', default=False, action='store_true',
            help='Calculates rate using wall time which can be helpful when clock is not published during simulation'
        )
        
        add_direct_node_arguments(parser)

    def main(self, *, args):
        return main(args)

class ROSTopicHz:
    """ROSTopicHz receives messages for a topic and computes frequency."""

    def __init__(self, node, window_size, filter_expr=None, use_wtime=False):
        self.lock = threading.Lock()
        self.msg_t0 = -1
        self.msg_tn = 0
        self.times = []
        self._last_printed_tn = defaultdict(int)
        self._msg_t0 = defaultdict(lambda: -1)
        self._msg_tn = defaultdict(int)
        self._times = defaultdict(list)
        self.filter_expr = filter_expr
        self.use_wtime = use_wtime
        self.window_size = window_size
        self._clock = node.get_clock()

    def get_last_printed_tn(self, topic=None):
        return self._last_printed_tn[topic] if topic else self.last_printed_tn

    def set_last_printed_tn(self, value, topic=None):
        if topic is None:
            self.last_printed_tn = value
        else:
            self._last_printed_tn[topic] = value

    def get_msg_t0(self, topic=None):
        return self._msg_t0[topic] if topic else self.msg_t0

    def set_msg_t0(self, value, topic=None):
        if topic is None:
            self.msg_t0 = value
        else:
            self._msg_t0[topic] = value

    def get_msg_tn(self, topic=None):
        return self._msg_tn[topic] if topic else self.msg_tn

    def set_msg_tn(self, value, topic=None):
        if topic is None:
            self.msg_tn = value
        else:
            self._msg_tn[topic] = value

    def get_times(self, topic=None):
        return self._times[topic] if topic else self.times

    def set_times(self, value, topic=None):
        if topic is None:
            self.times = value
        else:
            self._times[topic] = value

    def callback_hz(self, m, topic=None):
        """Calculate interval time for topic."""
        if self.filter_expr and not self.filter_expr(m):
            return
        
        with self.lock:
            curr_rostime = self._clock.now() if not self.use_wtime else Clock(clock_type=ClockType.SYSTEM_TIME).now()

            if curr_rostime.nanoseconds == 0:
                if len(self.get_times(topic=topic)) > 0:
                    print('Time has reset, resetting counters')
                    self.set_times([], topic=topic)
                return

            curr = curr_rostime.nanoseconds
            msg_t0 = self.get_msg_t0(topic=topic)

            if msg_t0 < 0 or msg_t0 > curr:
                self.set_msg_t0(curr, topic=topic)
                self.set_msg_tn(curr, topic=topic)
                self.set_times([], topic=topic)
            else:
                self.get_times(topic=topic).append(curr - self.get_msg_tn(topic=topic))
                self.set_msg_tn(curr, topic=topic)

            if len(self.get_times(topic=topic)) > self.window_size:
                self.get_times(topic=topic).pop(0)

    def get_hz(self, topic=None):
        """Calculate the average publishing rate."""
        if not self.get_times(topic=topic):
            return
        
        if self.get_last_printed_tn(topic=topic) == 0:
            self.set_last_printed_tn(self.get_msg_tn(topic=topic), topic=topic)
            return
        
        if self.get_msg_tn(topic=topic) < self.get_last_printed_tn(topic=topic) + 1e9:
            return

        with self.lock:
            times = self.get_times(topic=topic)
            n = len(times)
            mean = sum(times) / n
            rate = 1. / mean if mean > 0. else 0
            std_dev = math.sqrt(sum((x - mean)**2 for x in times) / n)
            max_delta = max(times)
            min_delta = min(times)

            self.set_last_printed_tn(self.get_msg_tn(topic=topic), topic=topic)

        return rate, min_delta, max_delta, std_dev, n

    def print_hz(self, topic=None):
        """Print the average publishing rate."""
        result = self.get_hz(topic)
        if result is None:
            return
        
        rate, min_delta, max_delta, std_dev, window = result
        print(f'average rate: {rate * 1e9:.3f}\n\tmin: {min_delta * 1e-9:.3f}s max: {max_delta * 1e-9:.3f}s std dev: {std_dev * 1e-9:.5f}s window: {window}')

def _rostopic_hz(update_cb, node, topic_list, window_size=DEFAULT_WINDOW_SIZE, filter_expr=None, use_wtime=False):
    """Print the publishing rate of a topic periodically."""
    qos_profile = QoSProfile(
        reliability=QoSReliabilityPolicy.BEST_EFFORT,
        history=QoSHistoryPolicy.KEEP_ALL,
        depth=window_size,
    )

    rt = ROSTopicHz(node, window_size, filter_expr=filter_expr, use_wtime=use_wtime)

    for topic in topic_list:
        msg_class = get_msg_class(node, topic, blocking=False, include_hidden_topics=False)

        if msg_class is None:
            print(f'{topic} is invalid')
            continue

        node.create_subscription(
            msg_class,
            topic,
            functools.partial(rt.callback_hz, topic=topic),
            qos_profile
        )

    while rclpy.ok():
        rclpy.spin_once(node)
        hz_dict = {}
        for topic in topic_list:
            hz = rt.get_hz(topic)
            if hz:
                hz_dict[topic] = hz[0] * 1e9
        if update_cb:
            update_cb(hz_dict)

    node.destroy_node()
    rclpy.shutdown()

def main(args, update_cb=None):
    topic_list = args.topic_list
    filter_expr = eval(args.filter_expr) if args.filter_expr else None

    with DirectNode(args) as node:
        _rostopic_hz(update_cb, node.node, topic_list, window_size=args.window_size, filter_expr=filter_expr, use_wtime=args.use_wtime)
