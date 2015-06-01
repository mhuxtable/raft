/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Implementation of a Raft server
 * @author Willem Thiart himself@willemthiart.com
 * @version 0.1
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

/* for varags */
#include <stdarg.h>

#include "raft.h"
#include "raft_log.h"
#include "raft_private.h"

enum loglevel
{
    DEBUG = 3,
    ALLBUTPERIODIC = 2,
    VERBOSE = 1,
    NONE = 0
};
static enum loglevel curlog = ALLBUTPERIODIC;
//static enum loglevel curlog = DEBUG;

void change_log_level(int inc)
{
	if (inc != -1 && inc != 0 && inc != 1)
		return;
	if (inc == -1 && curlog == 0)
		return;
	if (inc == 1 && curlog == 3)
		return;
	curlog += inc;
	
	char *loglevel;
	switch (curlog)
	{
		case DEBUG:
			loglevel = "DEBUG";
			break;
		case ALLBUTPERIODIC:
			loglevel = "ALLBUTPERIODIC";
			break;
		case VERBOSE:
			loglevel = "VERBOSE";
			break;
		case NONE:
			loglevel = "NONE";
			break;
		default:
			loglevel = "Out of bounds";
			break;
	}
	fprintf(stderr, "[Raft Engine] Log level is now %s\n", loglevel);
	return;
}

static void __log(enum loglevel l, raft_server_t *me_, const char *fmt, ...)
{
    if (NONE == l) return;
    if (curlog < l) return;

    char buf[1024];
    va_list args;

    va_start(args, fmt);
    vsprintf(buf, fmt, args);

//#if 0 /* debugging */
    raft_server_private_t* me = (raft_server_private_t*)me_;
    printf("%d: %s\n", me->nodeid, buf);
//    __FUNC_log(bto, src, buf);
//#endif
}

raft_server_t* raft_new()
{
    raft_server_private_t* me = (raft_server_private_t*)calloc(1, sizeof(raft_server_private_t));
    if (!me)
        return NULL;
    me->current_term = 0;
    me->voted_for = -1;
    me->current_idx = 0;
    me->timeout_elapsed = 0;
    me->request_timeout = 200;
    me->election_timeout = 1000;
    me->commit_idx = 0;
    me->log = log_new();
    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);
    return (raft_server_t*)me;
}

void raft_set_callbacks(raft_server_t* me_,
                        raft_cbs_t* funcs, void* udata)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    memcpy(&me->cb, funcs, sizeof(raft_cbs_t));
    me->udata = udata;
}

void raft_free(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    log_free(me->log);
    free(me_);
}

void raft_election_start(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(VERBOSE, me_, "election starting: %d %d, term: %d",
          me->election_timeout, me->timeout_elapsed, me->current_term);

    raft_become_candidate(me_);
}

void raft_become_leader(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    __log(VERBOSE, me_, "becoming leader");

    raft_set_state(me_, RAFT_STATE_LEADER);
    me->voted_for = -1;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->nodeid != i)
        {
            raft_node_t* p = raft_get_node(me_, i);
            raft_node_set_next_idx(p, raft_get_current_idx(me_) + 1);
            raft_send_appendentries(me_, i);
        }
    }
}

void raft_become_candidate(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    __log(VERBOSE, me_, "becoming candidate");

    memset(me->votes_for_me, 0, sizeof(int) * me->num_nodes);
    me->current_term += 1;
    raft_vote(me_, me->nodeid);
    raft_set_state(me_, RAFT_STATE_CANDIDATE);

    /* we need a random factor here to prevent simultaneous candidates */
    me->timeout_elapsed = rand() % 500;

    for (i = 0; i < me->num_nodes; i++)
        if (me->nodeid != i)
            raft_send_requestvote(me_, i);

}

void raft_become_follower(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(VERBOSE, me_, "becoming follower");

    raft_set_state(me_, RAFT_STATE_FOLLOWER);
    me->voted_for = -1;
}

int raft_periodic(raft_server_t* me_, int msec_since_last_period)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(DEBUG, me_, "periodic elapsed time: %d", me->timeout_elapsed);

    switch (me->state)
    {
    case RAFT_STATE_FOLLOWER:
        if (me->last_applied_idx < me->commit_idx)
            if (-1 == raft_apply_entry(me_))
                return -1;
        break;
    }

    me->timeout_elapsed += msec_since_last_period;

    if (me->state == RAFT_STATE_LEADER)
    {
        if (me->request_timeout <= me->timeout_elapsed)
        {
            raft_send_appendentries_all(me_);
            me->timeout_elapsed = 0;
        }
    }
    else if (me->election_timeout <= me->timeout_elapsed)
        raft_election_start(me_);

    return 0;
}

raft_entry_t* raft_get_entry_from_idx(raft_server_t* me_, int etyidx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    return log_get_from_idx(me->log, etyidx);
}

int raft_recv_appendentries_response(raft_server_t* me_,
                                     int node, msg_appendentries_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(ALLBUTPERIODIC, me_, "received appendentries response from: %d", node);

    raft_node_t* p = raft_get_node(me_, node);

    if (1 == r->success)
    {
        int i;

        for (i = r->first_idx; i <= r->current_idx; i++)
            log_mark_node_has_committed(me->log, i);

	// update the bookkeeping for the next index to send the node
	raft_node_set_next_idx(p, r->current_idx + 1);

        while (1)
        {
            raft_entry_t* e;

            e = log_get_from_idx(me->log, me->last_applied_idx + 1);

            /* majority has this */
            if (e && me->num_nodes / 2 <= e->num_nodes)
            {
		__log(ALLBUTPERIODIC , me_, "has a majority; applying to state machine\n");
                if (-1 == raft_apply_entry(me_))
                    break;
            }
            else
                break;
        }
    }
    else
    {
        /* If AppendEntries fails because of log inconsistency:
           decrement nextIndex and retry (§5.3) */
	__log(VERBOSE, me_, "Node %d refused AppendEntries.\n"
		"Notified first_idx %d, current_idx: %d for local actual %d\n"
		"raft_node local next_idx value is %d\n",
		node, r->first_idx, r->current_idx, raft_get_current_idx(me_),
		raft_node_get_next_idx(p));
        assert(0 <= raft_node_get_next_idx(p));
        // TODO does this have test coverage?
	// In CRAFT the node reports the last index, so we can use this to figure
	// out what log to send next (NOT part of the Raft algorithm proper).
        raft_node_set_next_idx(p, r->current_idx + 1);
        raft_send_appendentries(me_, node);
    }

    return 0;
}

int raft_recv_appendentries(
    raft_server_t* me_,
    const int node,
    msg_appendentries_t* ae,
    msg_appendentries_response_t *r
    )
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->timeout_elapsed = 0;
    __log(ALLBUTPERIODIC, me_, "received appendentries from: %d  ", node);

    r->term = me->current_term;

    /* we've found a leader who is legitimate */
    if (raft_is_leader(me_) && me->current_term <= ae->term)
        raft_become_follower(me_);

    /* 1. Reply false if term < currentTerm (§5.1) */
    if (ae->term < me->current_term)
    {
        __log(ALLBUTPERIODIC, me_, "AE term is less than current term");
        r->success = 0;
        return 0;
    }

//#if 0
    if (0 != ae->prev_log_idx &&
        ae->prev_log_idx < raft_get_current_idx(me_))
    {
        __log(ALLBUTPERIODIC, me_, "AE prev_idx is less than current idx");
        r->success = 0;
        return 0;
    }
//#endif

    /* not the first appendentries we've received */
    if (0 != ae->prev_log_idx)
    {
        raft_entry_t* e = raft_get_entry_from_idx(me_, ae->prev_log_idx);

        if (e)
        {
            /* 2. Reply false if log doesn't contain an entry at prevLogIndex
               whose term matches prevLogTerm (§5.3) */
            if (e->term != ae->prev_log_term)
            {
                __log(ALLBUTPERIODIC, me_, "AE term doesn't match prev_idx");
                r->success = 0;
                return 0;
            }

            /* 3. If an existing entry conflicts with a new one (same index
               but different terms), delete the existing entry and all that
               follow it (§5.3) */
            raft_entry_t* e2;

            e2 = raft_get_entry_from_idx(me_, ae->prev_log_idx + 1);

            if (e2)
                log_delete(me->log, ae->prev_log_idx + 1);
        }
        else
        {
            __log(ALLBUTPERIODIC, me_, "AE no log at prev_idx");
            r->success = 0;
            return 0;
        }
    }

    /* 5. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, last log index) */
    if (raft_get_commit_idx(me_) < ae->leader_commit)
    {
        raft_entry_t* e = log_peektail(me->log);
        if (e)
        {
            int id = e->id < ae->leader_commit ?  e->id : ae->leader_commit;
            raft_set_commit_idx(me_, id);
            while (0 == raft_apply_entry(me_))
                ;
        }
    }

    if (raft_is_candidate(me_))
        raft_become_follower(me_);

    raft_set_current_term(me_, ae->term);

    int i;

    /* append all entries to log */
    for (i = 0; i < ae->n_entries; i++)
    {
        msg_entry_t* cmd = &ae->entries[i];

        /* TODO: replace malloc with mempoll/arena */
        raft_entry_t* c = (raft_entry_t*)malloc(sizeof(raft_entry_t));
	memset(c, 0, sizeof(raft_entry_t));
        c->term = me->current_term;
        c->len = cmd->len;
        c->id = cmd->id;

	unsigned char *buf = (unsigned char *)malloc(cmd->len);
        memcpy(buf, cmd->data, cmd->len);
        c->data = buf;

        if (-1 == raft_append_entry(me_, c))
        {
            __log(ALLBUTPERIODIC, me_, "AE failure; couldn't append entry");
            r->success = 0;
            return -1;
        }
    }

    r->success = 1;
    r->current_idx = raft_get_current_idx(me_);
    r->first_idx = ae->prev_log_idx + 1;
    return 0;
}

int raft_recv_requestvote(raft_server_t* me_, int node, msg_requestvote_t* vr,
                          msg_requestvote_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (raft_get_current_term(me_) < vr->term)
        me->voted_for = -1;

    if (vr->term < raft_get_current_term(me_) ||
        /* we've already voted */
        -1 != me->voted_for ||
        /* we have a more up-to-date log */
        vr->last_log_idx < me->current_idx)
        r->vote_granted = 0;
    else
    {
        raft_vote(me_, node);
        r->vote_granted = 1;
    }

    __log(VERBOSE, me_, "node requested vote: %d replying: %s",
          node, r->vote_granted == 1 ? "granted" : "not granted");

    r->term = raft_get_current_term(me_);
    return 0;
}

int raft_votes_is_majority(const int num_nodes, const int nvotes)
{
    if (num_nodes < nvotes)
        return 0;
    int half = num_nodes / 2;
    return half + 1 <= nvotes;
}

int raft_recv_requestvote_response(raft_server_t* me_, int node,
                                   msg_requestvote_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(VERBOSE, me_, "node responded to requestvote: %d status: %s",
          node, r->vote_granted == 1 ? "granted" : "not granted");

    if (raft_is_leader(me_))
        return 0;

    assert(node < me->num_nodes);

//    if (r->term != raft_get_current_term(me_))
//        return 0;

    if (1 == r->vote_granted)
    {
        me->votes_for_me[node] = 1;
        int votes = raft_get_nvotes_for_me(me_);
        if (raft_votes_is_majority(me->num_nodes, votes))
            raft_become_leader(me_);
    }

    return 0;
}

int raft_recv_entry(raft_server_t* me_, int node, msg_entry_t* e,
                    msg_entry_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    raft_entry_t ety;
    int res, i;

    __log(ALLBUTPERIODIC, me_, "received entry from: %d", node);

    ety.term = me->current_term;
    ety.id = e->id;
    ety.data = (unsigned char *)malloc(e->len);
    memcpy(ety.data, e->data, e->len);
    ety.len = e->len;

    res = raft_append_entry(me_, &ety);
    for (i = 0; i < me->num_nodes; i++)
        if (me->nodeid != i)
            raft_send_appendentries(me_, i);

    r->id = e->id;
    r->was_committed = (0 == res);
    return 0;
}

int raft_send_requestvote(raft_server_t* me_, int node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    msg_requestvote_t rv;

    __log(ALLBUTPERIODIC, me_, "sending requestvote to: %d", node);

    rv.term = me->current_term;
    rv.last_log_idx = raft_get_current_idx(me_);
    if (me->cb.send_requestvote)
        me->cb.send_requestvote(me_, me->udata, node, &rv);
    return 0;
}

int raft_append_entry(raft_server_t* me_, raft_entry_t* c)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (0 == log_append_entry(me->log, c))
    {
        me->current_idx += 1;
        return 0;
    }
    return -1;
}

int raft_apply_entry(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    raft_entry_t* e;

    if (!(e = log_get_from_idx(me->log, me->last_applied_idx + 1)))
        return -1;

    if (me->state != RAFT_STATE_LEADER)
    {
	    __log(ALLBUTPERIODIC, me_, "now have a majority on entry %d %d",
			    e->id, me->last_applied_idx + 1);
    }
    __log(ALLBUTPERIODIC, me_, "applying log: %d", me->last_applied_idx);

    me->last_applied_idx++;
    if (me->commit_idx < me->last_applied_idx)
        me->commit_idx = me->last_applied_idx;
    if (me->cb.applylog)
        me->cb.applylog(me_, me->udata, e->data, e->len);
    return 0;
}

void raft_send_appendentries(raft_server_t* me_, int node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(DEBUG, me_, "sending appendentries to: %d", node);

    if (!(me->cb.send_appendentries))
        return;

    raft_node_t* p = raft_get_node(me_, node);

    // code which follows has been heavily modified because it was marked off
    // as "TODO" in the original implementation, which breaks the appendentries
    // RPC in the state machine
    msg_appendentries_t ae;

    /* need to figure out if we just need to send a heartbeat to this node --
       the behaviour when last log index in the leader < next index for
       follower -- or if we need to send one or more entries to this follower
       in order to advance the state machine. This is the case when last log
       index in the leader is >= next index in the follower. I do this as two
       separate steps primarily for debugging reasons, since I know a priori
       that the code for heartbeating works and does not lead to deadlocks, and
       would like to preserve this as is. */
    if (raft_get_current_idx(me_) < raft_node_get_next_idx(p))
    {
	    raft_entry_t *e = raft_get_entry_from_idx(me_, raft_node_get_next_idx(p)-1);
	    if (!e || me->current_idx == 0)
	    {
		    __log(DEBUG, me_, "1. %d %d\n", me->current_term, 0);
		    ae.prev_log_term = me->current_term;
		    ae.prev_log_idx  = 0;
	    } else {
		    __log(DEBUG, me_, "2. %d %d\n", e->term, e->id);
		    ae.prev_log_term = e->term;
		    ae.prev_log_idx  = e->id;
	    }
	    /* heartbeats only */
	    ae.term = me->current_term;
	    ae.leader_id = me->nodeid;
	    //ae.prev_log_term = 
	    // TODO:
	    //ae.prev_log_idx = 0;
	    ae.n_entries = 0;
	    ae.leader_commit = raft_get_commit_idx(me_);
	    me->cb.send_appendentries(me_, me->udata, node, &ae);
    }
    else
    {
	    /* need to send one or more log entries to the follower */
	    __log(VERBOSE, me_, "Sending appentries RPC to node %d  %d >= %d\n",
		node, raft_get_current_idx(me_), raft_node_get_next_idx(p));
	    raft_entry_t *e = raft_get_entry_from_idx(me_, raft_node_get_next_idx(p));
	    raft_entry_t *e2 = (e->id == 1 ? NULL :
			    raft_get_entry_from_idx(me_,
				    raft_node_get_next_idx(p) -
				    1));
	    ae.term = me->current_term;
	    ae.leader_id = me->nodeid;
	    ae.n_entries = 1;

	    msg_entry_t msg = { 0 };
	    msg.id = e->id;
	    msg.data = e->data;
	    msg.len  = e->len;
#ifdef DEBUG
	    fprintf(stderr, "sending %d %p %s\n", msg.id, msg.data, (char*)e->data);
#endif

	    ae.entries = &msg;
	    ae.leader_commit = raft_get_commit_idx(me_);
	    ae.prev_log_idx = raft_node_get_next_idx(p) - 1;
	    ae.prev_log_term = (e2 ? e2->term : me->current_term);
	    me->cb.send_appendentries(me_, me->udata, node, &ae);
	    return;
    }
}

void raft_send_appendentries_all(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    for (i = 0; i < me->num_nodes; i++)
        if (me->nodeid != i)
            raft_send_appendentries(me_, i);
}

void raft_set_configuration(raft_server_t* me_,
                            raft_node_configuration_t* nodes, int my_idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int num_nodes;

    /* TODO: one memory allocation only please */
    for (num_nodes = 0; nodes->udata_address; nodes++)
    {
        num_nodes++;
        me->nodes = (raft_node_t*)realloc(me->nodes, sizeof(raft_node_t*) * num_nodes);
        me->num_nodes = num_nodes;
        me->nodes[num_nodes - 1] = raft_node_new(nodes->udata_address);
    }
    me->votes_for_me = (int*)calloc(num_nodes, sizeof(int));
    me->nodeid = my_idx;
}

int raft_get_nvotes_for_me(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i, votes;

    for (i = 0, votes = 0; i < me->num_nodes; i++)
        if (me->nodeid != i)
            if (1 == me->votes_for_me[i])
                votes += 1;

    if (me->voted_for == me->nodeid)
        votes += 1;

    return votes;
}

void raft_vote(raft_server_t* me_, int node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    me->voted_for = node;
}
