import logging
from random import random
from time import sleep

from datetime import datetime
import numpy


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

    def __init__(self, name, dimension):
        super(Generator, self).__init__(name, count_inputs=0, dimension=dimension)

    def start(self):
        self.output.update(numpy.empty(self.dimension))


class RandomGenerator(Generator):

    def __init__(self, name, dimension):
        super(RandomGenerator, self).__init__(name, dimension=dimension)

    def start(self):
        while True:
            gen_ts = datetime.now()
            self.emit(gen_ts, numpy.array([random()] * self._dimension))
            sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')
    g1 = RandomGenerator('g1', 4)
    g1.attach('s1')

    b1 = TransferId('b1', 4)
    b1.chain(g1, 'input1')
    b1.attach('s2')

    b2 = TransferLogger('l1', 4)
    b2.chain(b1, 'input_logger')

    b3 = TransferLogger('l1', 4)
    b3.chain(b1, 'input_logger')

    g1.start()