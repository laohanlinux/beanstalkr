![build](https://github.com/laohanlinux/beanstalkr/workflows/build/badge.svg)

# beanstalkr

``` action
 put with delay               release with delay
  ----------------> [DELAYED] <------------.
                        |                   |
                        | (time passes)     |
                        |                   |
   put                  v     reserve       |       delete
  -----------------> [READY] ---------> [RESERVED] --------> *poof*
                       ^  ^                |  |
                       |   \  release      |  |
                       |    `-------------'   |
                       |                      |
                       | kick                 |
                       |                      |
                       |       bury           |
                    [BURIED] <---------------'
                       |
                       |  delete
                        `--------> *poof*
```

## Implement Commands

- [x] use
- [x] put
- [x] delete
- [x] bury
- [x] kick
- [x] kick-job
- [x] reserve
- [x] reserve-with-timeout
- [x] reserve-job
- [x] peek
- [x] peek-ready
- [x] peek-delayed
- [x] peek-buried
- [x] touch
- [x] watch
- [x] ignore
- [x] stats-job
- [x] stats-tube
- [x] stats
- [x] list-tubes
- [x] list-tube-used
- [x] list-tubes-watched
- [x] pause-tube
- [x] quit

## Protocol Compliance

This implementation follows the beanstalkd protocol specification:
- Full support for all producer commands (put, use)
- Full support for all worker commands (reserve, reserve-with-timeout, reserve-job, delete, release, bury, touch, watch, ignore)
- Full support for all inspection commands (peek, peek-ready, peek-delayed, peek-buried)
- Full support for all statistics commands (stats, stats-job, stats-tube, list-tubes, list-tube-used, list-tubes-watched)
- Full support for tube management (pause-tube, kick, kick-job)
- Proper error responses (OUT_OF_MEMORY, INTERNAL_ERROR, BAD_FORMAT, UNKNOWN_COMMAND, NOT_FOUND, NOT_IGNORED, TIMED_OUT, DEADLINE_SOON, DRAINING, BURIED)
- Tube name validation according to protocol specification
- Drain mode support (SIGUSR1 signal)
