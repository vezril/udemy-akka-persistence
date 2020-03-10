package playground.part4practices
import akka.actor.{ ActorLogging, ActorSystem, Props }
import akka.persistence.PersistentActor
import akka.persistence.journal.{ EventSeq, ReadEventAdapter }
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

object EventAdaptors extends App {

  // store for acoustic guitars

  val ACOUSTIC = "acoustic"
  val ELECTRIC = "electric"
  // data structures
  case class Guitar(id: String, model: String, make: String, guitarType: String = ACOUSTIC)
  // commands
  case class AddGuitar(guitar: Guitar, quantity: Int)
  //event
  case class GuitarAdded(guitarId: String, guitarModel: String, guitarMake: String, quantity: Int)
  case class GuitarAddedV2(guitarId: String, guitarModel: String, guitarMake: String, quantity: Int, guitarType: String)

  class InventoryManager extends PersistentActor with ActorLogging {
    override def persistenceId: String = "guitar-inventory-manager"

    val inventory: mutable.Map[Guitar, Int] = new mutable.HashMap[Guitar, Int]()

    override def receiveCommand: Receive = {
      case AddGuitar(guitar @ Guitar(id, model, make, guitarType), quantity) => {
        persistAsync(GuitarAddedV2(id, model, make, quantity, guitarType)) { e =>
          addGuitarInventory(guitar, quantity)
          log.info(s"Added $quantity x $guitar to inventory")
        }
      }
      case "print" => {
        log.info(s"Current inventory is: $inventory")
      }
    }
    override def receiveRecover: Receive = {
      case event @ GuitarAddedV2(id, model, make, quantity, guitarType) => {
        val guitar = Guitar(id, model, make, guitarType)
        addGuitarInventory(guitar, quantity)
        log.info(s"Recovered v2: $event")
      }
    }

    def addGuitarInventory(guitar: Guitar, quantity: Int): Unit = {
      val existingQuantity = inventory.getOrElse(guitar, 0)
      inventory.put(guitar, existingQuantity + quantity)
    }
  }

  class GuitarReadEventAdaptor extends ReadEventAdapter {
    /*
      journal -> serializer -> event adaptor -> actor
      (bytes)    (GA)          (GAv2)            (receiveRecover)
     */
    override def fromJournal(event: Any, manifest: String): EventSeq = event match {
      case GuitarAdded(guitarId, model, make, quantity) =>
        EventSeq.single(GuitarAddedV2(guitarId, model, make, quantity, ACOUSTIC))
      case other => EventSeq.single(other)
    }
  }
  // WriteEventAdapter -> used for backwards compatibility
  // actor -> write event adapter -> serializer -> journal

  val system           = ActorSystem("EventAdaptors", ConfigFactory.load().getConfig("eventAdaptors"))
  val inventoryManager = system.actorOf(Props[InventoryManager], "inventoryManager")

//  val guitars = for (i <- 1 to 10) yield Guitar(s"$i", "Hacker $i", "RockTheJVM")
//   guitars.foreach { guitar => inventoryManager ! AddGuitar(guitar, 5)
//  }
  inventoryManager ! "print"
}
