import logging
from collections import defaultdict
from random import random, gauss
from time import sleep
from datetime import datetime
import numpy
import sys


class Signal(object):
    def __init__(self, name, dimension):
        self._name = name
        self._value = numpy.empty(dimension)
        self._timestamp = None
        self._blocks = set()

    def add_block(self, block):
        self._blocks.add(block)

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, ts_value):
        logging.info('updated signal %s: %s', self._name, ts_value)
        self._timestamp, self._value = ts_value
        for block in self._blocks:
            block.on_update(self._timestamp, self)

    def __repr__(self):
        return '<Signal:%s:%s=%s>' % (self._timestamp, self._name, self._value)


class TransferBlock(object):
    def __init__(self, name, count_inputs, dimension):
        self._name = name
        self._inputs = dict()
        self._output = None
        self._count_inputs = count_inputs
        self.transfer = lambda x: numpy.empty(dimension)
        self._dimension = dimension

    def chain(self, block, input_name):
        assert block.output is not None, 'block signal undefined'
        assert len(self._inputs) < self._count_inputs
        self._inputs[input_name] = block.output
        logging.info('chained block %s to %s', block, self)
        block.output.add_block(self)

    def attach(self, signal_name):
        assert self._output is None, 'output signal already attached'
        signal = Signal(signal_name, self._dimension)
        self._output = signal

    def emit(self, emit_ts, value):
        self.output.value = (emit_ts, value)

    def on_update(self, update_ts, signal):
        # triggers computation
        self.emit(update_ts, self.transfer(signal.value))

    @property
    def output(self):
        return self._output

    @property
    def name(self):
        return self._name

    @property
    def dimension(self):
        return self._dimension

    def __repr__(self):
        return self._name


class TransferId(TransferBlock):
    def __init__(self, name, dimension):
        super(TransferId, self).__init__(name, count_inputs=1, dimension=dimension)
        self.transfer = lambda x: x


class TransferLogger(TransferBlock):
    def __init__(self, name, dimension):
        super(TransferLogger, self).__init__(name, count_inputs=1, dimension=dimension)

    def on_update(self, update_ts, signal):
        logging.info('signal update: %s', signal)


# todo
class TransferDelayed(TransferBlock):
    def __init__(self, name, count_inputs, dimension):
        super(TransferDelayed, self).__init__(name, count_inputs, dimension=dimension)


# todo
TransferSampler = TransferId


class Generator(TransferBlock):
    def __init__(self, sequencer, name, dimension):
        super(Generator, self).__init__(name, count_inputs=0, dimension=dimension)
        self._sequencer = sequencer

    @property
    def sequencer(self):
        return self._sequencer


class RandomRealtimeGenerator(Generator):
    def __init__(self, sequencer, name, dimension, count=10):
        super(RandomRealtimeGenerator, self).__init__(sequencer, name, dimension=dimension)
        self._count = count

    def start(self):
        def sequencer_callback(seq_ts):
            value = numpy.array(map(lambda f: f(), [random] * self._dimension))
            logging.info('emitting <%s, %s>', seq_ts, value)
            self.emit(seq_ts, value)

        while self._count:
            gen_ts = datetime.now()
            self.sequencer.expect(gen_ts, sequencer_callback)
            sleep(1)
            self._count -= 1


class DictGenerator(Generator):
    def __init__(self, sequencer, name, dict_stream):
        super(DictGenerator, self).__init__(sequencer, name, dimension=1)
        self._dict_stream = dict_stream

        def sequencer_callback(seq_ts):
            value = self._dict_stream[seq_ts]
            self.emit(seq_ts, value)

        for dict_ts in sorted(self._dict_stream.keys()):
            self.sequencer.expect(dict_ts, sequencer_callback)


class StreamSequencer(object):
    def __init__(self):
        self._expecting = defaultdict(set)

    def start(self):
        while self._expecting:
            next_deadline = min(self._expecting.keys())
            next_callbacks_in_line = self._expecting[next_deadline]
            for callback in next_callbacks_in_line:
                callback(next_deadline)

            self._expecting.pop(next_deadline, None)

    def expect(self, sequencer_ts, callback):
        logging.info('new expect received: %s', sequencer_ts)
        self._expecting[sequencer_ts].add(callback)


def go_rxpy():
    import rx
    from rx import Observable, Observer
    from rx.concurrency import Scheduler, AsyncIOScheduler
    from rx.subjects import Subject

    class LoggingObserver(Observer):
        def __init__(self, name):
            Observer.__init__(self)
            self._name = name

        def on_next(self, x):
            logging.info('<%s>received: %s', self._name, x)

        def on_error(self, e):
            logging.info('<%s>error: %s', self._name, e)

        def on_completed(self):
            logging.info('<%s>sequence completed', self._name)

    def uniform():
        while True:
            sleep(0.5)
            yield gauss(0., 1.)

    gen_white_noise = Observable.from_(uniform())

    def trf_shift(shift):
        return lambda value: value + shift

    def trf_scale(scale):
        return lambda value: scale * value

    white_noise_scaled = gen_white_noise.map(trf_scale(5.0)).map(trf_shift(10.0))
    white_noise_scaled.subscribe(LoggingObserver('white_noise_scaled'))
    gen_white_noise.subscribe(LoggingObserver('white_noise'))


def go_lusmu():
    import lusmu.core

    def trf_gaussian():
        return lambda dummy: gauss(0., 1.)

    def trf_sum():
        return lambda value1, value2: value1 + value2

    def trf_scale(intercept, slope):
        return lambda value: intercept + slope * value

    def trf_delay(initial=None):
        side_effect_trf_delay = {'previous': initial}

        def func_trf_delay(new_value, side_effect=side_effect_trf_delay):
            previous = side_effect['previous']
            side_effect['previous'] = new_value
            return previous

        return func_trf_delay

    def trf_cumul(initial=0.):
        side_effect_trf_cumul = {'previous': initial}

        def func_trf_cumul(new_value, cumul=side_effect_trf_cumul):
            cumul['previous'] += new_value
            return cumul['previous']

        return func_trf_cumul

    def trf_constant(constant=0.):
        return lambda dummy: constant

    def trf_linear(*factors):
        return lambda *values: sum([factor * value for factor, value in zip(factors, values)])

    def trf_logger(category):
        def func_trf_logger(value):
            logging.info('<%s>value=%s', category, value)

        return func_trf_logger

    class GraphBuilder(object):

        def __init__(self):
            self._nodes = dict()
            self._inputs_by_node = dict()
            self._clock = lusmu.core.Input('clock')

        def add_node(self, name, action, inputs=None):
            node = lusmu.core.Node(name=name, action=action)
            self._inputs_by_node[node] = inputs
            self._nodes[name] = node
            return node

        def _lookup_node(self, node_name):
            return self._nodes[node_name]

        def watch(self, node_name, watcher=trf_logger):
            node = self._lookup_node(node_name)
            lusmu.core.Node(action=trf_logger(node_name), inputs=lusmu.core.Node.inputs(node), triggered=True)

        def connect_inputs(self):
            for node in self._inputs_by_node.keys():
                # deferred assignment of inputs
                deferred_inputs = self._inputs_by_node[node]
                if deferred_inputs is None:
                    inputs = lusmu.core.Node.inputs(self._clock)

                else:
                    node_inputs = [self._lookup_node(node_name) for node_name in deferred_inputs]
                    inputs = lusmu.core.Node.inputs(*node_inputs)

                node.set_inputs(*inputs[0])

        def start(self):

            while True:
                lusmu.core.update_inputs([(builder._clock, datetime.now())])
                sleep(0.5)
                logging.info('---- next step ----')

    builder = GraphBuilder()
    builder.add_node('white_noise', action=trf_gaussian())
    builder.add_node('white_noise_scaled', action=trf_scale(0., 2.), inputs=['white_noise'])
    builder.add_node('drift', action=trf_constant(1.))
    builder.add_node('random_walk_increment', action=trf_sum(), inputs=['drift', 'white_noise_scaled'])
    builder.add_node('random_walk', action=trf_cumul(), inputs=['random_walk_increment'])
    builder.add_node('random_walk_ema_lag', action=trf_delay(initial=0.), inputs=['random_walk_ema'])
    builder.add_node('random_walk_ema', action=trf_linear(0.1, 0.9), inputs=['random_walk', 'random_walk_ema_lag'])

    builder.watch('random_walk')
    builder.watch('random_walk_ema')

    builder.connect_inputs()
    builder.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')
    go_lusmu()
    sys.exit(0)
