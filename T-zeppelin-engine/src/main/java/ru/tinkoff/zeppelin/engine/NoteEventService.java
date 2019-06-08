package ru.tinkoff.zeppelin.engine;

import java.util.List;
import java.util.stream.Collectors;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.NoteEvent;
import ru.tinkoff.zeppelin.core.notebook.NoteSubscription;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;
import ru.tinkoff.zeppelin.storage.NoteEventDAO;
import ru.tinkoff.zeppelin.storage.SchedulerDAO;

@Component
public class NoteEventService {

    private static final Logger LOG = LoggerFactory.getLogger(NoteEventService.class);


    private final NoteEventDAO noteEventDAO;
    private final NoteService noteService;
    private final SchedulerDAO schedulerDAO;
    private final String username;
    private final JavaMailSender emailSender;
    private final String textTemplateRun;
    private final String textTemplateError;
    private final String textTemplateScheduleChange;
    private final String server;
    private final String domain;

    public NoteEventService(final SchedulerDAO schedulerDAO,
                            final NoteEventDAO noteEventDAO,
                            final NoteService noteService,
                            final JavaMailSender emailSender,
                            @Value("${zeppelin.email.username}") final String username,
                            @Value("${zeppelin.email.textTemplate.run}") final String textTemplateRun,
                            @Value("${zeppelin.email.textTemplate.error}") final String textTemplateError,
                            @Value("${zeppelin.email.textTemplate.scheduleChange}") final String textTemplateScheduleChange,
                            @Value("${zeppelin.email.server}") final String server,
                            @Value("${zeppelin.email.domain}") final String domain) {
        this.schedulerDAO = schedulerDAO;
        this.noteEventDAO = noteEventDAO;
        this.noteService = noteService;
        this.username = username;
        this.emailSender = emailSender;
        this.textTemplateRun = textTemplateRun;
        this.textTemplateError = textTemplateError;
        this.textTemplateScheduleChange = textTemplateScheduleChange;
        this.server = server;
        this.domain = domain;
    }

    public void errorOnNoteScheduleExecution(final Job job) {
        final List<NoteSubscription> noteSubscriptions
                = noteEventDAO.getNoteEvent(job.getNoteId(), NoteEvent.Type.ERROR.toString());

        final List<String> mailingList = noteSubscriptions.stream()
                .filter(s -> s.getNotification() == NoteEvent.Notification.EMAIL)
                .map(s -> (s.getName() + domain))
                .collect(Collectors.toList());

        sendMail(mailingList, NoteEvent.Type.ERROR, noteService.getNote(job.getNoteId()), null);
    }

    public void successOnNoteScheduleExecution(final Long noteId) {
        final List<NoteSubscription> noteSubscriptions =
                noteEventDAO.getNoteEvent(noteId, NoteEvent.Type.RUN.toString());

        final List<String> mailingList = noteSubscriptions.stream()
                .filter(s -> s.getNotification() == NoteEvent.Notification.EMAIL)
                .map(s -> (s.getName() + domain))
                .collect(Collectors.toList());

        sendMail(mailingList, NoteEvent.Type.RUN, noteService.getNote(noteId), null);
    }

    public void noteScheduleChange(final Note note, final Scheduler oldScheduler) {
        final List<NoteSubscription> noteSubscriptions
                = noteEventDAO.getNoteEvent(note.getId(), NoteEvent.Type.SCHEDULE_CHANGE.name());

        final List<String> mailingList = noteSubscriptions.stream()
                .filter(s -> s.getNotification() == NoteEvent.Notification.EMAIL)
                .map(s -> (s.getName() + domain))
                .collect(Collectors.toList());

        sendMail(mailingList, NoteEvent.Type.SCHEDULE_CHANGE, note, oldScheduler);
    }

    private void sendMail(final List<String> mailTo, final NoteEvent.Type type, final Note note, final Scheduler oldScheduler) {
        if (mailTo.isEmpty()) {
            return;
        }
        final Scheduler currentScheduler = schedulerDAO.getByNote(note.getId());

        final MimeMessage mimeMessage = emailSender.createMimeMessage();

        final MimeMessageHelper message = new MimeMessageHelper(mimeMessage, "UTF-8");
        try {
            message.setFrom(username);
            message.setTo(mailTo.toArray(new String[0]));
            final String text;
            switch (type) {
                case RUN: {
                    message.setSubject("Notebook run on schedule");
                    text = textTemplateRun
                            .replace("{server}", server)
                            .replace("{note.uuid}", note.getUuid())
                            .replace("{note.path}", note.getPath())
                            .replace("{scheduler.expression}", currentScheduler.getExpression())
                            .replace("{scheduler.user}", currentScheduler.getUser())
                            .replace("{scheduler.nextExecution}", currentScheduler.getNextExecution().toString());
                    break;
                }
                case SCHEDULE_CHANGE: {
                    message.setSubject("Notebook schedule change");
                    text = textTemplateScheduleChange
                            .replace("{server}", server)
                            .replace("{note.uuid}", note.getUuid())
                            .replace("{note.path}", note.getPath())
                            .replace("{oldScheduler.expression}", oldScheduler.getExpression())
                            .replace("{newScheduler.enable}", currentScheduler.isEnabled() ? "True" : "False")
                            .replace("{newScheduler.expression}", currentScheduler.getExpression())
                            .replace("{oldScheduler.enable}", oldScheduler.isEnabled() ? "True" : "False")
                            .replace("{newScheduler.user}", currentScheduler.getUser())
                            .replace("{newScheduler.nextExecution}", currentScheduler.getNextExecution().toString());

                    break;
                }
                case ERROR: {
                    message.setSubject("Notebook execution error");
                    text = textTemplateError
                            .replace("{server}", server)
                            .replace("{note.uuid}", note.getUuid())
                            .replace("{note.path}", note.getPath())
                            .replace("{scheduler.expression}", currentScheduler.getExpression())
                            .replace("{scheduler.user}", currentScheduler.getUser())
                            .replace("{scheduler.nextExecution}", currentScheduler.getNextExecution().toString());
                    break;
                }
                default:
                    throw new RuntimeException("Unknown Event Type. Can't build message body.");

            }
            mimeMessage.setContent(text, "text/html");
            emailSender.send(mimeMessage);
        } catch (final MessagingException | RuntimeException exception) {
            LOG.info("Error on email send", exception);
        }
    }
}
