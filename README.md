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
- [x] peek
- [x] peek-ready
- [x] peek-delayed
- [x] peek-buried
- [x] touch
- [x] watch
- [x] ignore
- [ ] stats-job
- [ ] stats-tube
- [ ] stats
- [ ] list-tubes
- [ ] list-tube-used
- [x] list-tubes-watched
- [x] pause-tube
