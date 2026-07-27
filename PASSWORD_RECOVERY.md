# Password Recovery

## Endpoints

### `POST /api/forgotPassword`

Request:

```json
{
  "email": "user@example.com"
}
```

Response:

```json
{
  "message": "If an account exists for that email, a password reset link has been sent."
}
```

Notes:
- Response is intentionally generic.
- If outbound email is configured, the backend stores a hashed reset token and queues delivery of the reset email.
- Delivery is asynchronous, retried automatically on transient SMTP failures, and processed by every backend instance against the shared DB outbox.

### `POST /api/resetPassword`

Request:

```json
{
  "token": "raw-reset-token",
  "newPassword": "StrongPassword123"
}
```

Response:

```json
{
  "message": "Password reset successfully"
}
```

Notes:
- Reset tokens are single-use and expire automatically.
- All outstanding password reset tokens for the user are removed after a successful reset.
- A confirmation email is queued after the reset succeeds.

## Required configuration

When enabling outbound password recovery, set:

```env
MAIL_FROM_ADDRESS=no-reply@example.com
MAIL_FROM_NAME=Magpie
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_USERNAME=smtp-user
SMTP_PASSWORD=smtp-password
PUBLIC_APP_URL=https://magpie.example.com
```

Optional:

```env
EMAIL_BRAND_IMAGE_URL=https://magpie.example.com/magpie-email-header.png
PASSWORD_RESET_TOKEN_TTL_MINUTES=30
PASSWORD_RESET_CLEANUP_INTERVAL=1h
EMAIL_OUTBOX_POLL_INTERVAL=5s
EMAIL_OUTBOX_BATCH_SIZE=10
EMAIL_PROCESSING_TIMEOUT=15m
EMAIL_OUTBOX_RETENTION_HOURS=24
EMAIL_RETRY_BASE_SECONDS=5
EMAIL_MAX_ATTEMPTS=4
AUTH_FORGOT_PASSWORD_LIMIT_PER_EMAIL=1
AUTH_RESET_PASSWORD_LIMIT_PER_EMAIL=5
```

In multi-instance deployments, all backend instances can deliver queued emails in parallel. Outbox housekeeping (stale-message recovery and sent-row cleanup) remains leader-coordinated.

## Password policy

Password creation and reset currently require:
- minimum length `12`
- at least one lowercase letter
- at least one uppercase letter
- at least one number
- no whitespace

## Observability

Prometheus metrics:
- `magpie_email_delivery_total`
- `magpie_email_delivery_queue_depth`
