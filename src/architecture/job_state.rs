use super::job::State;
use super::error::TransitionError;

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
            let ok = (from == State::Reserved || from == State::Delayed || from == State::Buried);
            if ok {
                return Ok(true);
            }
            let txt: &'static str = Box::leak(format!("{}", from).into_boxed_str());
            return Err(TransitionError::Ready(txt));
        }
        State::Delayed => {
            let ok = (from == State::Reserved);
            if ok {
                return Ok(true);
            }
            return Err(TransitionError::Delayed(Box::leak(format!("{}", from).into_boxed_str())));
        }
        State::Reserved => {
            let ok = from == State::Ready;
            if ok {
                return Ok(true);
            }
            return Err(TransitionError::Reserved(Box::leak(format!("{}", from).into_boxed_str())));
        }
        State::Buried => {
            let ok = from == State::Reserved;
            if ok {
                return Ok(true);
            }
            return Err(TransitionError::Buried(Box::leak(format!("{}", from).into_boxed_str())));
        }
    }
}

