package playground.part2eventsourcing

import java.util.Date

import akka.actor.{ ActorLogging, ActorSystem, PoisonPill, Props }
import akka.persistence.PersistentActor

object PersistentActors extends App {

  /*
    Scenario: we have a business and an accountant which keeps track of our invoices.
   */

  // COMMAND
  case class Invoice(recipient: String, date: Date, amount: Int)
  case class InvoiceBult(invoices: List[Invoice])

  // Special messages
  case object Shutdown

  // EVENT
  case class InvoiceRecorded(id: Int, recipient: String, date: Date, amount: Int)

  class Accountant extends PersistentActor with ActorLogging {

    var latestInvoiceId = 0
    var totalAmount     = 0

    override def persistenceId: String = "simple-accountant"

    /**
      * This is the "normal" receive method
      */
    override def receiveCommand: Receive = {
      case Invoice(recipient, date, amount) =>
        /*
        When you receive a command
        1) you create an EVENT to persist into the store
        2) you persist the event, the pass in a callback that will get triggered once the event is written
        3) we update the actor's state when the event has persisted
         */
        log.info(s"Received invoice for amount: $amount")
        persist(InvoiceRecorded(latestInvoiceId, recipient, date, amount)) { e =>
          // SAFE to access mutalbe state here

          // update state
          latestInvoiceId += 1
          totalAmount += amount
          log.info(s"Persisted $e as invoice #${e.id}, for total amount $totalAmount")
        }
      case InvoiceBult(invoices) =>
        /*
          1) create events (plural)
          2) persist all events
          3) update the actor state when each event is persisted
         */
        val invoiceIds = latestInvoiceId to (latestInvoiceId + invoices.size)
        val events = invoices.zip(invoiceIds).map { pair =>
          val id      = pair._2
          val invoice = pair._1
          InvoiceRecorded(id, invoice.recipient, invoice.date, invoice.amount)
        }
        persistAll(events) { e =>
          latestInvoiceId += 1
          totalAmount += e.amount
          log.info(s"Persisted SINGLE $e as invoice #${e.id}, for total amount $totalAmount")
        }
      case Shutdown =>
        context.stop(self)
    }

    /**
      * Handler that will be called on recovery
      */
    override def receiveRecover: Receive = {
      /*
        best practice: Follow the logic in the persis steps of receiveCommand
       */
      case InvoiceRecorded(id, _, _, amount) =>
        log.info(s"Recovered invoice #$id for amount $amount, total amount $totalAmount")
        latestInvoiceId = 1
        totalAmount = amount
    }

    /*
      This method is called if persisting failed.
      The Actor will be STOPPED.

      Best practice: Start the actor again after a while
      (use Backoff supervisor)
     */
    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Fail to persis $event because of $cause")
      super.onPersistFailure(cause, event, seqNr)
    }

    /*
      Called if the JOURNAL fails to persist the event
      The actor is RESUMED
     */
    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Fail to persist $event because of $cause")
      super.onPersistRejected(cause, event, seqNr)
    }
  }

  val system     = ActorSystem("PersistentActors")
  val accountant = system.actorOf(Props[Accountant], "simpleAccountant")
//
//  for (i <- 1 to 10) {
//    accountant ! Invoice("The Sofa Company", new Date, i * 1000)
//  }

  /*
    Persistence failures
   */

  /**
    * Persisting multiple events
    *
    * persistAll
    */
  val newInvoices = for (i <- 1 to 5) yield Invoice("The awesome cahirs", new Date, i * 2000)
  //accountant ! InvoiceBult(newInvoices.toList)

  /*
    NEVER EVER CALL PERSIST OR PERSISTALL FROM FUTURES.
   */

  /**
    * Shutdown of persistent actors
    *
    * Vest practice: Define your own shutdown messages
    */
  //accountant ! PoisonPill
  accountant ! Shutdown // is handeled after all the current events ahve been completed
}
