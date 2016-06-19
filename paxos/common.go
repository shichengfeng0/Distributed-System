package paxos

const (
	OK = "OK"
	REJECT = "REJECT"
	DEAD_SERVER = "DEAD SERVER"
)

type Err string

type PrepareArgs struct {
	SeqNum				int
	ProposalNum 		int
}

type PrepareReply struct {
	Err					Err
	AcceptedProposalNum	int
	AcceptedValue		interface{}
	HighestProposalSeen int
	Decided				bool
}

type AcceptArgs struct {
	SeqNum				int
	ProposalNum 		int
	ProposalValue		interface{}
}

type AcceptReply struct {
	Err					Err
	AcceptedProposalNum	int
	HighestProposalSeen int
	Decided				bool
	DecidedValue		interface{}
}

type DecidedArgs struct {
	SeqNum				int
	DecidedProposalNum 	int
	DecidedValue		interface{}
}

type DecidedReply struct {
	Err					Err
	DecidedAlready		bool
	DecidedValue		interface{}
}

type GetHightestDoneSeqArgs struct {
}

type GetHightestDoneSeqReply struct {
	HightSeqDone 		int
	Err 				Err
}