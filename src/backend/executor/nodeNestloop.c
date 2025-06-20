/*-------------------------------------------------------------------------
 *
 * nodeNestloop.c
 *	  routines to support nest-loop joins
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNestloop.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecNestLoop	 - process a nestloop join of two plans
 *		ExecInitNestLoop - initialize the join
 *		ExecEndNestLoop  - shut down the join
 */

#include "postgres.h"
#include <stdio.h>
#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#define BLOCKSIZE 512
/* ----------------------------------------------------------------
 *		ExecNestLoop(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecNestLoop(PlanState *pstate)
{
	// you need to understand this trick to use inheritance in C to understand the typecast
	// the passed in PlanState *pstate is actually a pointer to a NestLoopState
	// the reason that you can visit the attribute of PlanState as you normally could
	// is that PlanState is the first attribute of Struct JoinState which is in turn
	// the first attribute of Struct NestLoopState
	// so the head of the NestLoopState is organized just like a PlanState
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	ListCell *lc;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nl = (NestLoop *)node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * Ok, everything is setup for the join so now loop until we return a
	 * qualifying join tuple.
	 */
	ENL1_printf("entering main loop");
	
	int i=0;
	int j=0;
	for (;;)
	{
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nl_NeedNewOuter)
		{
			ExecReScan(innerPlan);
			node->outerBlockSize = 0;
			node->outerBlockIndex = 0;
			ENL1_printf("getting new outer tuple");
			// elog(NOTICE, "getting New Outer");
			for(;node->outerBlockSize<BLOCKSIZE;node->outerBlockSize++){
				outerTupleSlot = ExecProcNode(outerPlan);
				if(!TupIsNull(outerTupleSlot))
					ExecCopySlot(node->outerBlock[node->outerBlockSize], outerTupleSlot);
				else
					break;				
			}
			
			
			ENL1_printf("saving new outer tuple information");
			// hack this is probably useful
			// econtext->ecxt_outertuple = outerTupleSlot;
			node->nl_NeedNewOuter = false;
		}
		if (node->nl_NeedNewInner)
		{
			node->innerBlockSize = 0;
			node->innerBlockIndex = 0;
			node->outerBlockIndex = 0;
			for(;node->innerBlockSize<BLOCKSIZE;node->innerBlockSize++){
				innerTupleSlot = ExecProcNode(innerPlan);
				if(!TupIsNull(innerTupleSlot))
					ExecCopySlot(node->innerBlock[node->innerBlockSize], innerTupleSlot);
				else{
					break; 
				}
			}
			node->nl_NeedNewInner = false;
		}

		//	for each r in outer block
		//		for each s in inner block
		//			test join condition	
		for(;node->outerBlockIndex<node->outerBlockSize;node->outerBlockIndex++){
			// elog(NOTICE, "innerIndex:%d",node->outerBlockIndex);
			for(;node->innerBlockIndex<node->innerBlockSize;node->innerBlockIndex++){
				econtext->ecxt_outertuple = node->outerBlock[node->outerBlockIndex];
				econtext->ecxt_innertuple = node->innerBlock[node->innerBlockIndex];
				if (ExecQual(joinqual, econtext))
				{	
					node->innerBlockIndex++;
					elog(NOTICE, "join condition satisfied");
					return ExecProject(node->js.ps.ps_ProjInfo);
				}
				else{
					ResetExprContext(econtext);
				}
			}
			node->innerBlockIndex =0;
		}
		

	// new outer/inner block needed and join finish logic

		// after the nested for loop a few lines above we definitely need a new inner block
		node->nl_NeedNewInner =true;
		// notice: node->innerBlockSize could be zero
		// if the last innerBlock is not full
		// then we have traversed through the inner table for current outerBlock
		// so it's time to start a new outer block(which will also handle the reScan of inner table)
		if(node->innerBlockSize!=BLOCKSIZE){ 
			node->nl_NeedNewOuter =true;
		}

		// if the last outerBlock is not full
		// then we are at the last outer block
		// so when the innerBlock is also not full
		// we know we have traversed throught the inner table for the last block
		if(node->outerBlockSize!=BLOCKSIZE && node->innerBlockSize!=BLOCKSIZE){
			elog(NOTICE, "finish since the outerBlockSize does not match BlockSize");
			elog(NOTICE, "join condition is tested for: %d",i);
			return NULL;
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState *
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags)
{
	NestLoopState *nlstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NL1_printf("ExecInitNestLoop: %s\n",
			   "initializing node");

	/*
	 * create state structure
	 */
	// just like new in C++
	nlstate = makeNode(NestLoopState);
	nlstate->js.ps.plan = (Plan *)node;
	nlstate->js.ps.state = estate;
	nlstate->js.ps.ExecProcNode = ExecNestLoop;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &nlstate->js.ps);

	/*
	 * initialize child nodes
	 *
	 * If we have no parameters to pass into the inner rel from the outer,
	 * tell the inner child that cheap rescans would be good.  If we do have
	 * such parameters, then there is no point in REWIND support at all in the
	 * inner child, because it will always be rescanned with fresh parameter
	 * values.
	 */
	outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
	if (node->nestParams == NIL)
		eflags |= EXEC_FLAG_REWIND;
	else
		eflags &= ~EXEC_FLAG_REWIND;
	innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(&nlstate->js.ps, &TTSOpsVirtual);
	ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

	/*
	 * initialize child expressions
	 */
	nlstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *)nlstate);
	nlstate->js.jointype = node->join.jointype;
	nlstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *)nlstate);

	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	nlstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
	case JOIN_INNER:
	case JOIN_SEMI:
		break;
	default:
		elog(ERROR, "unrecognized join type: %d",
			 (int)node->join.jointype);
	}

	/*
	 * finally, wipe the current outer tuple clean.
	 */
	nlstate->nl_NeedNewOuter = true;
	nlstate->nl_NeedNewInner = true;
	// 250620 get space for the block(a array of pointer to TupleTableSlot)

	nlstate->outerBlock = (TupleTableSlot **)palloc0fast(BLOCKSIZE * sizeof(TupleTableSlot *));
	nlstate->innerBlock = (TupleTableSlot **)palloc0fast(BLOCKSIZE * sizeof(TupleTableSlot *));
	for (int i = 0; i < BLOCKSIZE; i++)
	{
		// 为每个元素创建一个独立的、可写的虚拟元组槽
		nlstate->outerBlock[i] = ExecInitNullTupleSlot(estate, ExecGetResultType(outerPlanState(nlstate)), &TTSOpsVirtual);
	}
	for (int i = 0; i < BLOCKSIZE; i++)
	{
		nlstate->innerBlock[i] = ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(nlstate)), &TTSOpsVirtual);
	}
	nlstate->outerBlockSize = 0;
	nlstate->innerBlockSize = 0;
	nlstate->outerBlockIndex = 0;
	nlstate->innerBlockIndex = 0;

	NL1_printf("ExecInitNestLoop: %s\n",
			   "node initialized");

	return nlstate;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void ExecEndNestLoop(NestLoopState *node)
{
	NL1_printf("ExecEndNestLoop: %s\n",
			   "ending node processing");
	pfree(node->outerBlock);
	pfree(node->innerBlock);
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	NL1_printf("ExecEndNestLoop: %s\n",
			   "node processing ended");
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void ExecReScanNestLoop(NestLoopState *node)
{
	PlanState *outerPlan = outerPlanState(node);

	/*
	 * If outerPlan->chgParam is not null then plan will be automatically
	 * re-scanned by first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);

	/*
	 * innerPlan is re-scanned for each new outer tuple and MUST NOT be
	 * re-scanned from here or you'll get troubles from inner index scans when
	 * outer Vars are used as run-time keys...
	 */

	node->nl_NeedNewOuter = true;
}
