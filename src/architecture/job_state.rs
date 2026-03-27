use super::error::TransitionError;
use super::job::State;

/**

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
*/
pub fn is_valid_transitions_to(from: State, to: State) -> Result<bool, TransitionError> {
    match to {
        State::Ready => {
            let ok = from == State::Reserved || from == State::Delayed || from == State::Buried;
            if ok {
                return Ok(true);
            }
            let txt: &'static str = Box::leak(format!("{}", from).into_boxed_str());
            Err(TransitionError::Ready(txt))
        }
        State::Delayed => {
            let ok = from == State::Reserved;
            if ok {
                return Ok(true);
            }
            Err(TransitionError::Delayed(Box::leak(
                format!("{}", from).into_boxed_str(),
            )))
        }
        State::Reserved => {
            // reserve-job 命令允许从 Ready、Buried 或 Delayed 状态转换到 Reserved
            let ok = from == State::Ready || from == State::Buried || from == State::Delayed;
            if ok {
                return Ok(true);
            }
            Err(TransitionError::Reserved(Box::leak(
                format!("{}", from).into_boxed_str(),
            )))
        }
        State::Buried => {
            let ok = from == State::Reserved;
            if ok {
                return Ok(true);
            }
            Err(TransitionError::Buried(Box::leak(
                format!("{}", from).into_boxed_str(),
            )))
        }
    }
}
