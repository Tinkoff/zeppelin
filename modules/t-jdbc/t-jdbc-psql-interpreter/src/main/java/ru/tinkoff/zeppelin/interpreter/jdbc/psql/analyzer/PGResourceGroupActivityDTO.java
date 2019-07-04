package ru.tinkoff.zeppelin.interpreter.jdbc.psql.analyzer;

import java.time.LocalDateTime;
import java.util.StringJoiner;
import ru.tinkoff.zeppelin.commons.jdbc.analyzer.ResourceDTO;

// https://postgrespro.ru/docs/postgrespro/11/monitoring-stats#PG-STAT-ACTIVITY-VIEW
public class PGResourceGroupActivityDTO implements ResourceDTO {

  /**
   * Тип события, которого ждёт обслуживающий процесс,
   * если это имеет место; в противном случае — NULL.
   * Возможные значения:
   *       LWLock:    обслуживающий процесс ожидает лёгкую блокировку. Такие блокировки защищают определённые структуры данных в разделяемой памяти. В wait_event будет содержаться имя, отражающее цель получения лёгкой блокировки. (Некоторые блокировки имеют особые имена; другие объединяются в группы блокировок со схожим предназначением.)
   *       Lock:      Обслуживающий процесс ожидает тяжёлую блокировку. Тяжёлые блокировки, также называемые блокировками менеджера блокировок или просто блокировками, в основном защищают объекты уровня SQL, такие как таблицы. Однако они также применяются для взаимоисключающего выполнения некоторых внутренних операций, например, для расширения отношений. Тип ожидаемой блокировки показывается в wait_event.
   *       BufferPin: Серверный процесс ожидает доступа к буферу данных, когда никакой другой процесс не обращается к этому буферу. Ожидание закрепления буфера может растягиваться, если другой процесс удерживает открытый курсор, который читал данные из нужного буфера.
   *       Activity:  Серверный процесс простаивает. Это состояние наблюдается в системных процессах, ожидающих активности в основном цикле обработки. В wait_event обозначается конкретное место ожидания.
   *       Extension: Серверный процесс ожидает активности в модуле расширения. Эта категория полезна при использовании модулей, она помогает отслеживать нестандартные места ожидания.
   *       Client:    Серверный процесс ожидает в сокете некоторую активность пользовательского приложения, и события, ожидаемые сервером, не зависят от его внутренних процессов. В wait_event обозначается конкретное место ожидания.
   *       IPC:       Серверный процесс ожидает некоторой активности другого процесса на сервере. В wait_event обозначается конкретное место ожидания.
   *       Timeout:   Серверный процесс ожидает истечения определённого времени. В wait_event обозначается конкретное место ожидания.
   *       IO:        Серверный процесс ожидает завершения операции ввода/вывода. В wait_event обозначается конкретное место ожидания.
   */
  public enum PGWaitEventType {
    LWLOCK,
    LOCK,
    BUFFERPIN,
    ACTIVITY,
    EXTENSION,
    CLIENT,
    IPC,
    TIMEOUT,
    IO;
  }

  /**
   * Общее текущее состояние этого серверного процесса. Возможные значения:
   *       active:                        серверный процесс выполняет запрос.
   *       idle:                          серверный процесс ожидает новой команды от клиента.
   *       idle in transaction:           серверный процесс находится внутри транзакции, но в настоящее время не выполняет никакой запрос.
   *       idle in transaction (aborted): Это состояние подобно idle in transaction, за исключением того, что один из операторов в транзакции вызывал ошибку.
   *       fastpath function call:        серверный процесс выполняет fast-path функцию.
   *       disabled:                      Это состояние отображается для серверных процессов, у которых параметр track_activities отключён.
   */
  public enum PGEventState {
    ACTIVE,
    IDLE,
    IDLE_IN_TRANSACTION,
    IDLE_IN_TRANSACTION_ABORTED,
    FASTPATH_FUNCTION_CALL,
    DISABLED;
  }

  /**
   * Тип текущего серверного процесса.
   * Возможные варианты:
   * autovacuum launcher,
   * autovacuum worker,
   * logical replication launcher,
   * logical replication worker,
   * parallel worker,
   * background writer,
   * client backend,
   * checkpointer,
   * startup,
   * walreceiver,
   * walsender
   * walwriter.
   * Кроме того, фоновые рабочие процессы, регистрируемые расширениями, могут иметь дополнительные типы.
   */
  public enum PGBackendType {
    AUTOVACUUM_LAUNCHER,
    AUTOVACUUM_WORKER,
    LOGICAL_REPLICATION_LAUNCHER,
    LOGICAL_REPLICATION_WORKER,
    PARALLEL_WORKER,
    BACKGROUND_WRITER,
    CLIENT_BACKEND,
    CHECKPOINTER,
    STARTUP,
    WALRECEIVER,
    WALSENDER,
    WALWRITER;
  }

  /**
   * Тип	                Описание
   * oid	                OID базы данных, к которой подключён этот серверный процесс
   */
  private long datid;

  /**
   * Тип	                Описание
   * name	              Имя базы данных, к которой подключён этот серверный процесс
   */
  private String datname;

  /**
   * Тип	                Описание
   * integer	            Идентификатор процесса этого серверного процесса
   */
  private long pid;

  /**
   * Тип	                Описание
   * oid	                OID пользователя, подключённого к этому серверному процессу
   */
  private long usesysid;

  /**
   * Тип	                Описание
   * name	              Имя пользователя, подключённого к этому серверному процессу
   */
  private String usename;


  /**
   * Тип	                Описание
   * text	              Название приложения, подключённого к этому серверному процессу
   */
  private String application_name;


  /**
   * Столбец	              Тип	                Описание
   * client_addr	          inet	              IP-адрес клиента, подключённого к этому серверному процессу.
   *                                            Значение null в этом поле означает, что клиент подключён
   *                                            через сокет Unix на стороне сервера или
   *                                            что это внутренний процесс, например, автоочистка.
   */
  private String client_addr;

  /**
   * Столбец	              Тип	                Описание
   * client_hostname	      text	              Имя компьютера для подключённого клиента, получаемое
   *                                            в результате обратного поиска в DNS по client_addr.
   *                                            Это поле будет отлично от null только в случае соединений
   *                                            по IP и только при включённом режиме log_hostname.
   */
  private String client_hostname;

  /**
   * Столбец	              Тип	                Описание
   * client_port	         integer	            Номер TCP-порта, который используется клиентом для
   *                                            соединения с этим серверным процессом, или -1,
   *                                            если используется сокет Unix
   */
  private long client_port;

  /**
   * Столбец	              Тип	                Описание
   * backend_start	  timestamp with time zone	Время запуска процесса. Для процессов, обслуживающих
   *                                            клиентов, это время подключения клиента к серверу.
   */
  private LocalDateTime backend_start;

  /**
   * Столбец	              Тип	                Описание
   * xact_start	      timestamp with time zone	Время начала текущей транзакции в этом процессе или null
   *                                            при отсутствии активной транзакции. Если текущий запрос
   *                                            был первым в своей транзакции, то значение в этом
   *                                            столбце совпадает со значением столбца query_start.
   */
  private LocalDateTime xact_start;

  /**
   * Столбец	              Тип	                Описание
   * query_start	    timestamp with time zone	Время начала выполнения активного в данный момент запроса,
   *                                            или, если state не active, то время начала выполнения
   *                                            последнего запроса
   */
  private LocalDateTime query_start;

  /**
   * Столбец	              Тип	                Описание
   * state_change	    timestamp with time zone	Время последнего изменения состояния (поля state)
   */
  private LocalDateTime state_change;

  /**
   * Столбец	              Тип	                Описание
   * wait_event_type	     text	                Тип события, которого ждёт обслуживающий процесс,
   *                                            если это имеет место; в противном случае — NULL.
   */
  private String wait_event_type;

  /**
   * Столбец	              Тип	                Описание
   * wait_event	            text	              Имя ожидаемого события, если обслуживающий процесс
   *                                            находится в состоянии ожидания, а в противном случае
   *                                            — NULL. За подробностями обратитесь к Таблице 27.4.
   */
  private String wait_event;

  /**
   * Столбец	              Тип	                Описание
   * state	                text	              Общее текущее состояние этого серверного процесса.
   */
  private String state;

  /**
   * Столбец	              Тип	                Описание
   * backend_xid	          xid               	Идентификатор верхнего уровня транзакции этого серверного процесса или любой другой.
   */
  private long backend_xid;

  /**
   * Столбец	              Тип	                Описание
   * backend_xmin	          xid	                текущая граница xmin для серверного процесса.
   */
  private long backend_xmin;

  /**
   * Столбец	              Тип	                Описание
   * query	               text	                Текст последнего запроса этого серверного процесса.
   *                                            Если state имеет значение active, то в этом поле
   *                                            отображается запрос, который выполняется в настоящий момент.
   *                                            Если процесс находится в любом другом состоянии,
   *                                            то в этом поле отображается последний выполненный запрос.
   *                                            По умолчанию текст запроса обрезается до 1024 символов;
   *                                            это число определяется параметром track_activity_query_size.
   */
  private String query;

  /**
   *  Тип	                Описание
   *  text	                Тип текущего серверного процесса. Возможные варианты: ...
   *                      Кроме того, фоновые рабочие процессы, регистрируемые расширениями,
   *                      могут иметь дополнительные типы.
   */
  private String backend_type;

  public PGResourceGroupActivityDTO(final long datid,
                                    final String datname,
                                    final long pid,
                                    final long usesysid,
                                    final String usename,
                                    final String application_name,
                                    final String client_addr,
                                    final String client_hostname,
                                    final long client_port,
                                    final LocalDateTime backend_start,
                                    final LocalDateTime xact_start,
                                    final LocalDateTime query_start,
                                    final LocalDateTime state_change,
                                    final String wait_event_type,
                                    final String wait_event,
                                    final String state,
                                    final long backend_xid,
                                    final long backend_xmin,
                                    final String query,
                                    final String backend_type) {
    this.datid = datid;
    this.datname = datname;
    this.pid = pid;
    this.usesysid = usesysid;
    this.usename = usename;
    this.application_name = application_name;
    this.client_addr = client_addr;
    this.client_hostname = client_hostname;
    this.client_port = client_port;
    this.backend_start = backend_start;
    this.xact_start = xact_start;
    this.query_start = query_start;
    this.state_change = state_change;
    this.wait_event_type = wait_event_type;
    this.wait_event = wait_event;
    this.state = state;
    this.backend_xid = backend_xid;
    this.backend_xmin = backend_xmin;
    this.query = query;
    this.backend_type = backend_type;
  }

  @Override
  public long getResourceId() {
    return pid;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{",
        "}")
        .add("datid=" + datid)
        .add("datname='" + datname + "'")
        .add("pid=" + pid)
        .add("usesysid=" + usesysid)
        .add("usename='" + usename + "'")
        .add("application_name='" + application_name + "'")
        .add("client_addr='" + client_addr + "'")
        .add("client_hostname='" + client_hostname + "'")
        .add("client_port=" + client_port)
        .add("backend_start=" + backend_start)
        .add("xact_start=" + xact_start)
        .add("query_start=" + query_start)
        .add("state_change=" + state_change)
        .add("wait_event_type='" + wait_event_type + "'")
        .add("wait_event='" + wait_event + "'")
        .add("state='" + state + "'")
        .add("backend_xid=" + backend_xid)
        .add("backend_xmin=" + backend_xmin)
        .add("query='" + query + "'")
        .add("backend_type='" + backend_type + "'")
        .toString();
  }
}
