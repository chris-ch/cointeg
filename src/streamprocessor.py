import logging
from random import gauss
from time import sleep

import rx
asyncio = rx.config['asyncio']

from rx import Observable, Observer
from rx.concurrency import AsyncIOScheduler
from rx.observable import Observable
from rx.internal import extensionmethod


@extensionmethod(Observable)
def to_async_generator(self, future_ctor=None, sentinel=None):
    future_ctor = future_ctor or rx.config.get("Future")
    if not future_ctor:
        raise Exception('Future type not provided nor in rx.config.Future')

    loop = asyncio.get_event_loop()
    future = [future_ctor()]
    notifications = []

    def feeder():
        if not len(notifications) or future[0].done():
            return

        notification = notifications.pop(0)

        if notification.kind == "E":
            future[0].set_exception(notification.exception)
        elif notification.kind == "C":
            future[0].set_exception(StopIteration(sentinel))
        else:
            future[0].set_result(notification.value)

    def on_next(value):
        """Takes on_next values and appends them to the notification queue"""

        notifications.append(value)
        loop.call_soon(feeder)

    self.materialize().subscribe(on_next)

    @asyncio.coroutine
    def gen():
        """Generator producing futures"""

        loop.call_soon(feeder)
        future[0] = future_ctor()

        return future[0]
    return gen


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


@asyncio.coroutine
def go():

    def uniform():
        while True:
            sleep(0.5)
            yield gauss(0., 1.)

    gen_white_noise = Observable.from_(uniform(), scheduler=AsyncIOScheduler())

    def trf_shift(shift):
        return lambda value: value + shift

    def trf_scale(scale):
        return lambda value: scale * value

    white_noise_scaled = gen_white_noise.map(trf_scale(5.0)).map(trf_shift(10.0))
    white_noise_scaled.subscribe(LoggingObserver('white_noise_scaled'))
    gen_white_noise.subscribe(LoggingObserver('white_noise'))
    gen = white_noise_scaled.to_async_generator()

    # Wish we could write something like:
    # ys = (x for x in yield from gen())
    while True:
        x = yield from gen()
        if x is None:
            break


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(go())

if __name__ == '__main__':
    main()