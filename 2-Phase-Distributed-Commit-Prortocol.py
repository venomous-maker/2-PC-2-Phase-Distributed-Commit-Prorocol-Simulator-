import argparse
import threading
import random
import time

class TransactionCoordinator:
    def __init__(self):
        # Initialize the coordinator with empty participant and transaction state dictionaries
        self.participants = {}
        self.transaction_state = {}
        self.timeout = 5  # Default timeout in seconds

    def add_participant(self, participant_number, participant):
        # Add a participant to the coordinator
        self.participants[participant_number] = participant

    def start_transaction(self, client_numbers, participant_count, transaction_ids, timeout, fail_before_prepare=True, fail_after_one_commit=True):
        # Start a new transaction
        for transaction_id in transaction_ids:
            print(f"Starting transaction {transaction_id}")
            # Initialize transaction state as INIT
            self.transaction_state[transaction_id] = "INIT"
            # Start a new thread for preparing the transaction
            t = threading.Thread(target=self.send_prepare, args=(client_numbers, participant_count, transaction_id, timeout, fail_before_prepare, fail_after_one_commit))
            t.start()

    def send_prepare(self, client_numbers, participant_count, transaction_id, timeout, fail_before_prepare, fail_after_one_commit):
        # Send prepare message to all participants
        print(f"Coordinator sending prepare message for transaction {transaction_id}")
        responses = {}
        for participant_number, participant in self.participants.items():
            response = participant.receive_prepare(transaction_id, client_numbers, participant_count, timeout, fail_before_prepare, fail_after_one_commit)
            responses[participant_number] = response
        # Sleep to simulate network latency
        time.sleep(timeout)
        # Handle responses received
        self.handle_prepare_response(transaction_id, responses)

    def handle_prepare_response(self, transaction_id, responses):
        # Handle responses received from participants
        if all(response == "YES" for response in responses.values()):
            print(f"Coordinator (Transaction {transaction_id}) received all prepare responses, sending commit message")
            # If all participants respond with YES, send commit message
            self.send_commit(transaction_id)
        elif any(response == "NO" for response in responses.values()):
            print(f"Coordinator (Transaction {transaction_id}) received NO prepare response(s), aborting transaction")
            # If any participant responds with NO, abort transaction
            self.abort_transaction(transaction_id)
        else:
            print(f"Coordinator (Transaction {transaction_id}) timed out waiting for prepare responses, aborting transaction")
            # If timeout occurs, abort transaction
            self.abort_transaction(transaction_id)

    def send_commit(self, transaction_id):
        # Send commit message to all participants
        print(f"Coordinator sending commit message for transaction {transaction_id}")
        for participant_number, participant in self.participants.items():
            participant.receive_commit(transaction_id)
        print(f"Transaction {transaction_id} committed")
        # Wait for all participants to finish committing
        for participant_number, participant in self.participants.items():
            participant.wait_commit_finish(transaction_id)
        # Remove transaction from transaction state after committing
        self.transaction_state.pop(transaction_id, None)
        time.sleep(1)  # Simulate disk write delay

    def abort_transaction(self, transaction_id):
        # Abort the transaction
        print(f"Transaction {transaction_id} aborted")
        for participant_number, participant in self.participants.items():
            participant.abort_transaction(transaction_id)
        self.transaction_state.pop(transaction_id, None)

class Participant:
    def __init__(self, participant_number, coordinator):
        # Initialize participant with participant number and coordinator reference
        self.participant_number = participant_number
        self.coordinator = coordinator
        self.connection = None

    def connect(self, connection):
        # Connect participant to a connection
        self.connection = connection

    def receive_prepare(self, transaction_id, client_numbers, participant_count, timeout, fail_before_prepare, fail_after_one_commit):
        # Receive prepare message from coordinator
        delay = random.randint(0, 10)
        time.sleep(delay)
        print(f"Participant {self.participant_number} received prepare message for transaction {transaction_id} from clients {client_numbers}")
        if fail_before_prepare:
            print(f"Participant {self.participant_number} failing before responding to prepare message")
            return "NO"
        response = "YES" if self.participant_number in client_numbers and random.random() < 0.8 else "NO"
        print(f"Participant {self.participant_number} responding with", response)
        if response == "NO":
            self.coordinator.transaction_state[transaction_id] = "ABORTED"
        else:
            self.coordinator.transaction_state[transaction_id] = "PREPARED"
            if fail_after_one_commit and self.participant_number == 1:
                print(f"Participant {self.participant_number} failing after replying YES")
                time.sleep(timeout + 1)
        return response

    def receive_commit(self, transaction_id):
        # Receive commit message from coordinator
        delay = random.randint(0, 10)
        time.sleep(delay)
        print(f"Participant {self.participant_number} received commit message for transaction {transaction_id}")
        if self.coordinator.transaction_state.get(transaction_id) == "PREPARED":
            print(f"Participant {self.participant_number} committing transaction {transaction_id}")
        else:
            print(f"Participant {self.participant_number} received unexpected commit message")

    def wait_commit_finish(self, transaction_id):
        # Wait for the participant to finish committing
        pass

    def abort_transaction(self, transaction_id):
        # Abort the transaction
        if self.connection:
            print(f"Participant {self.participant_number} closing connection for transaction {transaction_id}")
            # close connection here
            self.connection = None

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Simulate 2-phase distributed commit protocol with failure scenarios')
    parser.add_argument('--clients', nargs='+', type=int, required=True, help='List of client numbers')
    parser.add_argument('--participant-count', type=int, default=2, help='Number of participants')
    parser.add_argument('--transaction-ids', nargs='+', type=int, required=True, help='List of transaction IDs')
    parser.add_argument('--timeout', type=int, default=5, help='Timeout in seconds')
    parser.add_argument('--sleep-time', type=int, default=15, help='Sleep time in seconds')
    parser.add_argument('--fail-before-prepare', dest='fail_before_prepare', action='store_true', help='Simulate failure before responding to prepare message')
    parser.add_argument('--no-fail-before-prepare', dest='fail_before_prepare', action='store_false', help='Do not simulate failure before responding to prepare message')
    parser.add_argument('--fail-after-one-commit', dest='fail_after_one_commit', action='store_true', help='Simulate failure after replying YES to prepare message')
    parser.add_argument('--no-fail-after-one-commit', dest='fail_after_one_commit', action='store_false', help='Do not simulate failure after replying YES to prepare message')

    args = parser.parse_args()

    # Create a new coordinator instance
    coordinator = TransactionCoordinator()
    # Create participant instances and add them to the coordinator
    participants = {i: Participant(i, coordinator) for i in range(1, args.participant_count + 1)}
    for client in args.clients:
        participants[client].connect(client) # Assign connections to participants
        coordinator.add_participant(client, participants[client])

    # Start transactions
    coordinator.start_transaction(args.clients, args.participant_count, args.transaction_ids, args.timeout, args.fail_before_prepare, args.fail_after_one_commit)
    time.sleep(args.sleep_time)  # Let transactions complete

if __name__ == "__main__":
    main()

