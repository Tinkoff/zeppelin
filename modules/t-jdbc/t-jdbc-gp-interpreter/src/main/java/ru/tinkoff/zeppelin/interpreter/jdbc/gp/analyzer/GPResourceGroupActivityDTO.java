package ru.tinkoff.zeppelin.interpreter.jdbc.gp.analyzer;

import java.time.LocalDateTime;
import java.util.StringJoiner;
import ru.tinkoff.zeppelin.commons.jdbc.analyzer.ResourceDTO;

// https://gpdb.docs.pivotal.io/500/ref_guide/system_catalogs/pg_stat_activity.html
public class GPResourceGroupActivityDTO implements ResourceDTO {

  /**
   * type	references	description
   * oid	pg_database.oid	Database OID
   */
  private long datid;

  /**
   * type	      references	        description
   * name	 	        -               Database name
   */
  private String datname;

  /**
   * type	      references	        description
   * integer	   	                  Process ID of the server process
   */
  private int procpid;

  /**
   * type	      references	        description
   * integer	   	                  Session ID
   */
  private int sess_id;

  /**
   * type	      references	        description
   * oid	        pg_authid.oid	    Role OID
   */
  private long usesysid;

  /**
   * type	      references	        description
   * name	 	                        Role name
   */
  private String usename;

  /**
   * type	      references	        description
   * text	 	                        Current query that process is running
   */
  private String current_query;

  /**
   * type	      references	        description
   * boolean	 	                      True if waiting on a lock, false if not waiting
   */
  private boolean waiting;

  /**
   * type	      references	        description
   * timestamptz	 	                  Time query began execution
   */
  private LocalDateTime query_start;

  /**
   * type	      references	        description
   * timestamptz	 	                  Time backend process was started
   */
  private LocalDateTime backend_start;

  /**
   * type	      references	        description
   * inet	 	                        Client address
   */
  private String client_addr;

  /**
   * type	      references	        description
   * integer	 	                      Client port
   */
  private int client_port;

  /**
   * type	      references	        description
   * text	 	                        Client application name
   */
  private String application_name;

  /**
   * type	      references	        description
   * timestamptz	 	                Transaction start time
   */
  private LocalDateTime xact_start;

  /**
   * type	      references	        description
   * text	 	                        Reason the server process is waiting. The value can be:
   *                                lock, replication, or resgroup
   */
  private String waiting_reason;

  /**
   * type	      references	        description
   * oid       	pg_resgroup.oid	    Resource group OID
   */
  private long rsgid;

  /**
   * type	      references	        description
   * text	      pg_resgroup.rsgname	Resource group name
   */
  private String rsgname;

  /**
   * type	      references	        description
   * interval	 	                    For a queued query, the total time the query has been queued.
   */
  private long rsgqueueduration;

  public GPResourceGroupActivityDTO(final long datid,
                                    final String datname,
                                    final int procpid,
                                    final int sess_id,
                                    final long usesysid,
                                    final String usename,
                                    final String current_query,
                                    final boolean waiting,
                                    final LocalDateTime query_start,
                                    final LocalDateTime backend_start,
                                    final String client_addr,
                                    final int client_port,
                                    final String application_name,
                                    final LocalDateTime xact_start,
                                    final String waiting_reason,
                                    final long rsgid,
                                    final String rsgname,
                                    final long rsgqueueduration) {
    this.datid = datid;
    this.datname = datname;
    this.procpid = procpid;
    this.sess_id = sess_id;
    this.usesysid = usesysid;
    this.usename = usename;
    this.current_query = current_query;
    this.waiting = waiting;
    this.query_start = query_start;
    this.backend_start = backend_start;
    this.client_addr = client_addr;
    this.client_port = client_port;
    this.application_name = application_name;
    this.xact_start = xact_start;
    this.waiting_reason = waiting_reason;
    this.rsgid = rsgid;
    this.rsgname = rsgname;
    this.rsgqueueduration = rsgqueueduration;
  }

  @Override
  public long getResourceId() {
    return procpid;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ","{", "}")
        .add("datid=" + datid)
        .add("datname='" + datname + "'")
        .add("procpid=" + procpid)
        .add("sess_id=" + sess_id)
        .add("usesysid=" + usesysid)
        .add("usename='" + usename + "'")
        .add("current_query='" + current_query + "'")
        .add("waiting=" + waiting)
        .add("query_start=" + query_start)
        .add("backend_start=" + backend_start)
        .add("client_addr='" + client_addr + "'")
        .add("client_port=" + client_port)
        .add("application_name='" + application_name + "'")
        .add("xact_start=" + xact_start)
        .add("waiting_reason='" + waiting_reason + "'")
        .add("rsgid=" + rsgid)
        .add("rsgname='" + rsgname + "'")
        .add("rsgqueueduration=" + rsgqueueduration)
        .toString();
  }
}



















