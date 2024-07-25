start_server {tags {needs:repl external:skip}} {
    start_server {} {
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]

        r replicaof $primary_host $primary_port
        wait_for_condition 50 100 {
            [s 0 master_link_status] eq {up}
        } else {
            fail "Replicas not replicating from primary"
        }

        test {replica allow read command by default} {
            r get foo
        } {}

        test {replica reply READONLY error for write command by default} {
            assert_error {READONLY*} {r set foo bar}
        }

        test {replica redirect read and write command after CLIENT CAPA REDIRECT} {
            r client capa redirect
            assert_error "REDIRECT $primary_host:$primary_port" {r set foo bar}
            assert_error "REDIRECT $primary_host:$primary_port" {r get foo}
        }

        test {non-data access commands are not redirected} {
            r ping
        } {PONG}

        test {replica allow read command in READONLY mode} {
            r readonly
            r get foo
        } {}

        test {erroneous failover-in-progress state responses} {
            set primary [srv -1 client]
            set replica [srv 0 client]
            set replica_host [srv 0 host]
            set replica_port [srv 0 port]

            $primary client capa redirect
            $primary wait 1 0

            set pd [valkey_deferring_client -1]
            $pd client capa redirect
            $pd read ; # Consume the OK reply
            $pd blpop list 0

            pause_process [srv 0 pid]

            $primary failover to $replica_host $replica_port timeout 100 force

            # The paused replica is fully synced (and even if not, we force
            # the failover-in-progress state).
            wait_for_condition 50 100 {
                [s -1 master_failover_state] == "failover-in-progress"
            } else {
                fail "primary not in failover-in-progress state"
            }

            # The primary is waiting for the PSYNC FAILOVER response now.
            # A writing command should block the client, but issues a redirect.
            assert_error "REDIRECT $replica_host:$replica_port" {$primary set foo bar}

            $primary readonly

            $primary config set slave-serve-stale-data no
            # Reading a non-existing key should return nil, but issues a MASTERDOWN
            assert_error "*MASTERDOWN*" {$primary get foo}
            $primary config set slave-serve-stale-data yes

            # Client with capa redirect in a blocking call that was active before the failover gets a
            # UNBLOCKED response _before_ the replica becomes the new primary.
            # Expected: Client should still be blocked at this moment. REDIRECT response shall be 
            # sent when the new primary is ready, i.e. when unblocking all clients after failover (or,
            # if the failover failed, just keep those clients blocked)
            assert_error "*UNBLOCKED*" {$pd read}

            # Verify that we were in failover-in-progress state the whole time
            assert_equal "failover-in-progress" [s -1 master_failover_state]

            resume_process [srv 0 pid]
        }

        test {correct waiting-for-sync state responses} {
            wait_for_condition 50 100 {
                [s -1 master_failover_state] == "no-failover"
            } else {
                fail "primary not in no-failover state"
            }

            $primary replicaof no one
            r replicaof $primary_host $primary_port
            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replicas not replicating from primary"
            }

            $primary client capa redirect

            pause_process [srv 0 pid]

            $primary set foo bar

            $primary failover

            # The replica is not synced and sleeps. Primary should be waiting for
            # sync.
            assert_equal "waiting-for-sync" [s -1 master_failover_state]

            # Reading a key should return its value
            $primary config set slave-serve-stale-data yes
            assert_equal bar [$primary get foo]
            $primary config set slave-serve-stale-data no
            assert_equal bar [$primary get foo]

            # Primary is in waiting-for-sync still
            assert_match "*master_failover_state:waiting-for-sync*" [$primary info replication]

            # No client should be blocked
            assert_equal 0 [s -1 blocked_clients]

            set pd [valkey_deferring_client -1]
            $pd client capa redirect
            $pd read ; # Consume the OK reply
            $pd set foo bar

            # Writing command blocks the client ...
            wait_for_blocked_clients_count 1 100 10 -1

            resume_process [srv 0 pid]

            # ... and after the failover, gets a redirect
            assert_error "REDIRECT $replica_host:$replica_port" {$pd read}

            # Writing command blocked until failover finished
            assert_equal "no-failover" [s -1 master_failover_state]

            $pd close
        }
    }
}
