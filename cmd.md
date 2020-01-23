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
- [ ] peek
- [ ] peek-ready
- [ ] peek-delayed
- [ ] peek-buried
- [ ] touch
- [x] watch
- [x] ignore
- [ ] stats-job
- [ ] stats-tube
- [ ] stats
- [ ] list-tubes
- [ ] list-tube-used
- [ ] list-tubes-watched
- [ ] pause-tube