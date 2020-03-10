package playground.part3storesSerialization
import akka.actor.ActorLogging
import akka.persistence._

class SimplePersistentActor extends PersistentActor with ActorLogging {
  override def persistenceId: String = "simple-persistent-actor"

  // mutable state
  var nMessages = 0

  override def receiveCommand: Receive = {
    case "print" => {
      log.info(s"I have persisted $nMessages")
    }
    case "snap" => {
      saveSnapshot(nMessages)
    }
    case SaveSnapshotSuccess(metadata) => {
      log.info(s"Save snapshot success: $metadata")
    }
    case SaveSnapshotFailure(_, cause) => {
      log.warning(s"Save snapshot failed: $cause")
    }
    case message => {
      persist(message) { _ => log.info(s"Persisting $message")
      }
      nMessages += 1
    }
  }

  override def receiveRecover: Receive = {
    case RecoveryCompleted => {
      log.info("Recovery done")
    }
    case SnapshotOffer(_, paylaod: Int) => {
      log.info(s"Recovered snapshot: $paylaod")
      nMessages = paylaod
    }
    case messages => {
      log.info(s"Recovered: $messages")
      nMessages += 1
    }
  }
}
