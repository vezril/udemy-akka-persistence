package playground.part2eventsourcing
import java.util.UUID

import akka.actor.{ ActorLogging, ActorSystem, Props }
import akka.persistence.PersistentActor

import scala.collection.mutable

object PersistentActorExercise extends App {

  /*
    Persistent actor for a voting station
    Keep:
      - the citizens who voted
      - the poll: mapping between a candidate and the number of received votes

    The actor must be able to recover its state if it's shutdown or restarted
   */

  // Commands
  case class Vote(citizenPID: String, candidate: String)
  case object Results
  case object Initialize

  // Events
  case class VoteEvent(id: UUID, citizenPID: String, candidate: String)

  case object VoteAccepted
  case object VoteRejected

  class VotingBooth extends PersistentActor with ActorLogging {
    override def receiveCommand: Receive = inactive

    private def inactive: Receive = {
      case Initialize => context.become(active(List(), List()))
    }

    private def active(citizens: List[String], candidates: List[String]): Receive = {
      case Vote(citizenPID, candidate) => {
        // Means he already voted
        if (citizens.exists(_ == citizenPID)) {
          log.info(s"Citizen ${citizenPID} has already voted")
        } else {
          persist(VoteEvent(UUID.randomUUID(), citizenPID, candidate)) { e =>
            log.info(s"Citizen ${e.citizenPID} has  voted for ${e.candidate}")
            context.become(active(citizens ++ List(e.citizenPID), candidates ++ List(e.candidate)))
          }
        }
      }
    }

    override def receiveRecover: Receive =
      ??? //case VoteEvent(_, citizenPID, candidate) => context.become()
    override def persistenceId: String = "voting-booth-actor"
  }

  case class VoteRecorded(citizenPID: String, candidate: String)

  class VotingStation extends PersistentActor with ActorLogging {

    val citizens: mutable.Set[String]  = new mutable.HashSet[String]()
    val poll: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()

    override def persistenceId: String = "simple-voting-station"
    override def receiveCommand: Receive = {
      case vote @ Vote(citizenPID, candidate) =>
        if (!citizens.contains(vote.citizenPID)) {
          persist(vote) { _ => // COMMAND sourcing
            log.info(s"Persisted: $vote")
            handleInternalStateChange(citizenPID, candidate)
          }
        }

      case "print" =>
        log.info(s"Current state: \nCitizens: ${citizens}\npolls: $poll")
    }

    def handleInternalStateChange(citizenPID: String, candidate: String): Unit = {
      citizens.add(citizenPID)
      val votes = poll.getOrElse(candidate, 0)
      poll.put(candidate, votes + 1)
    }

    override def receiveRecover: Receive = {
      case vote @ Vote(citizenPID, candidate) =>
        log.info(s"Recovered: $vote")
        handleInternalStateChange(citizenPID, candidate)
    }
  }

  val system        = ActorSystem("PersistentActorExercise")
  val votingStation = system.actorOf(Props[VotingStation], "votingStation")

  val votesMap = Map[String, String](
    "Alice"   -> "Martin",
    "Bob"     -> "Roland",
    "Charlie" -> "Martin",
    "David"   -> "Jonas",
    "Daniel"  -> "Martin"
  )

//  votesMap.keys.foreach { citizen => votingStation ! Vote(citizen, votesMap(citizen))
//  }

  votingStation ! "print"
}
