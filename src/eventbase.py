import logging
from collections import defaultdict
from random import random
from time import sleep

from datetime import datetime, timedelta
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
        sequencer.register(self)

    @property
    def sequencer(self):
        return self._sequencer

    def start(self):
        pass

    def sequencer_callback(self, seq_ts):
        pass


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



class StepGenerator(Generator):

    def __init__(self, sequencer, name, step_time):
        super(StepGenerator, self).__init__(sequencer, name, dimension=1)
        self._step_time = step_time

    def start(self):
        self.sequencer.expect(self._step_time, self.sequencer_callback)

    def sequencer_callback(self, seq_ts):
        self.emit(seq_ts, 1)


class DictGenerator(Generator):

    def __init__(self, sequencer, name, dict_stream):
        super(DictGenerator, self).__init__(sequencer, name, dimension=1)
        self._dict_stream = dict_stream

    def start(self):
        def sequencer_callback(seq_ts):
            value = self._dict_stream[seq_ts]
            self.emit(seq_ts, value)
            
        for dict_ts in sorted(self._dict_stream.keys()):
            self.sequencer.expect(dict_ts, sequencer_callback)
    

class StreamSequencer(object):

    def __init__(self):
        self._expecting = defaultdict(set)
        self._generators = set()

    def register(self, generator):
        self._generators.add(generator)

    def start(self):
        for generator in self._generators:
            generator.start()

    def expect(self, sequencer_ts, callback):
        logging.info('new expect received: %s', sequencer_ts)
        self._expecting[sequencer_ts].add(callback)
        next_deadline = min(self._expecting.keys())
        next_callbacks_in_line = self._expecting[next_deadline]
        for callback in next_callbacks_in_line:
            callback(next_deadline)
            
        self._expecting.pop(next_deadline, None)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')
    # b1 = TransferId('b1', 4)
    # b1.chain(g1, 'input1')
    # b1.attach('s2')
    #
    # b2 = TransferLogger('l1', 4)
    # b2.chain(b1, 'input_logger')
    #
    # b3 = TransferLogger('l1', 4)
    # b3.chain(b1, 'input_logger')

    seq = StreamSequencer()
    gen1 = RandomRealtimeGenerator(seq, 'gen1', dimension=4, count=3)
    gen1.attach('s1')
    l1 = TransferLogger('l1', 4)
    l1.chain(gen1, 'gen1_logger')
    
    gen_stream = {
        datetime(2012, 1, 1): 1.,
        datetime(2013, 1, 1): 2.,
        datetime(2014, 1, 1): 3.,
        datetime(2015, 1, 1): 4.,
        datetime(2016, 1, 1): 5.,
        datetime(2017, 1, 1): 6.,
    }
    gen2 = DictGenerator(seq, 'gen2', gen_stream)
    gen2.attach('s2')
    l2 = TransferLogger('l2', 4)
    l2.chain(gen2, 'gen2_logger')
    
    seq.start()
