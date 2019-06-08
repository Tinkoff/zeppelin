package ru.tinkoff.zeppelin.engine;

import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.Credential;
import ru.tinkoff.zeppelin.storage.CredentialsDAO;


/**
 * Service for operations on credentials
 *
 * @author Egor Klimov
 * @version 1.1
 * @since 1.1
 */
@Component
public class CredentialService {

  private final CredentialsDAO credentialsDAO;

  public CredentialService(final CredentialsDAO credentialsDAO) {
    this.credentialsDAO = credentialsDAO;
  }

  @Nonnull
  public Set<Credential> getUserOwnedCredentials(@Nonnull final String username) {
    return credentialsDAO.getAllCredentials()
        .stream()
        .filter(c -> c.getOwners().contains(username))
        .collect(Collectors.toSet());
  }

  @Nonnull
  public Set<Credential> getUserReadableCredentials(@Nonnull final String username) {
    return credentialsDAO.getAllCredentials()
        .stream()
        .filter(c -> c.getReaders().contains(username))
        .collect(Collectors.toSet());
  }

  @Nullable
  public Credential getCredential(@Nonnull final String key) {
    return credentialsDAO.get(key);
  }

  @Nullable
  public Credential getCredential(final long credentialId) {
    return credentialsDAO.get(credentialId);
  }

  @Nullable
  public Credential addReader(@Nonnull final String key, @Nonnull final String reader) {
    final Credential dist = getCredential(key);
    if (dist != null) {
      dist.getReaders().add(reader);
      return updateCredential(dist);
    }
    return null;
  }

  @Nullable
  public Credential addOwner(@Nonnull final String key, @Nonnull final String reader) {
    final Credential dist = getCredential(key);
    if (dist != null) {
      dist.getOwners().add(reader);
      dist.getReaders().add(reader);
      return updateCredential(dist);
    }
    return null;
  }

  @Nonnull
  public Credential persistCredential(@Nonnull final Credential credential) {
    return credentialsDAO.persist(credential);
  }

  @Nonnull
  public Credential updateCredential(@Nonnull final Credential credential) {
    return credentialsDAO.update(credential);
  }

  public void deleteCredential(final long credentialId) {
    credentialsDAO.remove(credentialId);
  }

  public void deleteCredential(@Nonnull final String key) {
    credentialsDAO.remove(key);
  }
}
