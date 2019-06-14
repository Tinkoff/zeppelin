package ru.tinkoff.zeppelin.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.notebook.*;
import ru.tinkoff.zeppelin.storage.NoteEventDAO;
import ru.tinkoff.zeppelin.storage.SchedulerDAO;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.util.List;
import java.util.stream.Collectors;

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
    private final Boolean notificationEnableFlg;

    public NoteEventService(SchedulerDAO schedulerDAO,
                            NoteEventDAO noteEventDAO,
                            NoteService noteService,
                            JavaMailSender emailSender,
                            @Value("${zeppelin.email.username}") final String username,
                            @Value("${zeppelin.email.textTemplate.run}") final String textTemplateRun,
                            @Value("${zeppelin.email.textTemplate.error}") final String textTemplateError,
                            @Value("${zeppelin.email.textTemplate.scheduleChange}") final String textTemplateScheduleChange,
                            @Value("${zeppelin.email.server}") final String server,
                            @Value("${zeppelin.email.domain}") final String domain,
                            @Value("${zeppelin.notification.enable}") final Boolean notificationEnableFlg) {
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
        this.notificationEnableFlg = notificationEnableFlg;
    }

    public void errorOnNoteScheduleExecution(Job job) {
        final List<NoteSubscription> note_subscriptions
                = noteEventDAO.getNoteEvent(job.getNoteId(), NoteEvent.Type.ERROR.toString());

        final List<String> mailing_list = note_subscriptions.stream()
                .filter(s -> s.getNotification() == NoteEvent.Notification.EMAIL)
                .map(s -> (s.getName() + domain))
                .collect(Collectors.toList());

        sendMail(mailing_list, NoteEvent.Type.ERROR, noteService.getNote(job.getNoteId()), null);
    }

    public void successOnNoteScheduleExecution(Long noteId) {
        List<NoteSubscription> note_subscriptions =
                noteEventDAO.getNoteEvent(noteId, NoteEvent.Type.RUN.toString());

        List<String> mailing_list = note_subscriptions.stream()
                .filter(s -> s.getNotification() == NoteEvent.Notification.EMAIL)
                .map(s -> (s.getName() + domain))
                .collect(Collectors.toList());

        sendMail(mailing_list, NoteEvent.Type.RUN, noteService.getNote(noteId), null);
    }

    public void noteScheduleChange(Note note, Scheduler oldScheduler) {
        List<NoteSubscription> note_subscriptions
                = noteEventDAO.getNoteEvent(note.getId(), NoteEvent.Type.SCHEDULE_CHANGE.name());

        List<String> mailing_list = note_subscriptions.stream()
                .filter(s -> s.getNotification() == NoteEvent.Notification.EMAIL)
                .map(s -> (s.getName() + domain))
                .collect(Collectors.toList());

        sendMail(mailing_list, NoteEvent.Type.SCHEDULE_CHANGE, note, oldScheduler);
    }

    private void sendMail(final List<String> mailTo, final NoteEvent.Type type, final Note note, final Scheduler oldScheduler) {
        if (!notificationEnableFlg || mailTo.isEmpty()) {
            return;
        }
        final Scheduler currentScheduler = schedulerDAO.getByNote(note.getId());

        final MimeMessage mimeMessage = emailSender.createMimeMessage();

        final MimeMessageHelper message = new MimeMessageHelper(mimeMessage, "UTF-8");
        try {
            message.setFrom(username);
            message.setTo(mailTo.toArray(new String[0]));
            String text;
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
        } catch (MessagingException | RuntimeException exception) {
            LOG.info("Error on email send: " + exception.getMessage());
        }
    }
}
