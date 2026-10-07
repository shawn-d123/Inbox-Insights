# Data dictionary

Generated from `src/inbox_insights/governance.py`, which is also what writes the
Unity Catalog comments and tags. Columns marked PII are tagged `pii = true`.

## `bronze.raw_emails`

Kaggle Enron emails.csv loaded unchanged, one row per mailbox file.

| Column | Description | PII |
|---|---|---|
| `file` | Source path in the original maildir, e.g. allen-p/sent_mail/1. |  |
| `message` | Raw email: headers, blank line, body. | yes |
| `source_path` | File in the Volume this row was read from. |  |
| `ingested_at` | When the row was loaded. |  |

## `silver.emails`

Parsed, cleaned and de-duplicated emails. One row per unique email.

| Column | Description | PII |
|---|---|---|
| `message_id` | Message-ID header, without angle brackets. Primary key. |  |
| `file` | Source file of the copy that was kept after de-duplication. |  |
| `mailbox` | Mailbox owner folder, e.g. allen-p. |  |
| `x_folder` | Original mail client folder path. |  |
| `source_system` | lotus_notes, outlook or unknown, from X-FileName. |  |
| `sent_at_header` | Date header as written. Shifted for Lotus Notes exports. |  |
| `sent_at` | Corrected send time (UTC instant). |  |
| `sent_at_houston` | Corrected send time as Houston wall-clock time. |  |
| `sender` | From address, lower case. | yes |
| `to_addresses` | To addresses, lower case. | yes |
| `cc_addresses` | Cc addresses, lower case. Bcc is dropped: it always copies Cc. | yes |
| `subject` | Subject line. |  |
| `subject_normalised` | Subject lower-cased with Re:/Fw: prefixes removed. |  |
| `is_reply` | Subject starts with Re:. |  |
| `is_forward` | Subject starts with Fw: or Fwd:. |  |
| `body_clean` | Body with quoted replies, forwards and signatures removed. | yes |
| `body_full` | Decoded body including forwarded text; used for topic flags. | yes |
| `body_hash` | SHA-256 of the raw body; part of the de-duplication key. |  |

## `silver.recipients`

One row per email and To/Cc recipient.

| Column | Description | PII |
|---|---|---|
| `message_id` | Email this recipient belongs to. |  |
| `recipient` | Recipient address, lower case. | yes |
| `recipient_type` | to or cc. An address in both is kept once, as to. |  |

## `silver.dq_quarantine`

Parsed rows that failed a quality rule, kept with the reason instead of dropped.

| Column | Description | PII |
|---|---|---|
| `reason` | Rules failed, '; ' separated: missing_message_id, unparseable_date, date_out_of_range, missing_sender. |  |
| `quarantined_at` | When the row was quarantined. |  |

Also tagged PII: `sender`, `to_addresses`, `cc_addresses`, `bcc_addresses`, `body`

## `silver.run_metrics`

Row counts at each pipeline stage, appended on every run.

| Column | Description | PII |
|---|---|---|
| `metric` | Stage name, e.g. bronze_rows or after_content_dedup. |  |
| `value` | Row count. |  |
| `run_at` | When the run recorded the count. |  |

## `gold.weekly_volume`

Emails per week, split by whether they stayed inside Enron.

| Column | Description | PII |
|---|---|---|
| `week_start` | Monday of the week, Houston time. |  |
| `total_emails` | Unique emails sent that week. |  |
| `internal_emails` | Sent by an Enron address to Enron addresses only. |  |
| `external_emails` | Any other email, including Bcc-only ones. |  |

## `gold.email_categories`

Monthly share of meeting, after-hours and weekend email.

| Column | Description | PII |
|---|---|---|
| `month` | First day of the month, Houston time. |  |
| `total_emails` | Unique emails sent that month. |  |
| `meeting_emails` | Emails matching the meeting keyword rule. |  |
| `meeting_share` | meeting_emails / total_emails. |  |
| `after_hours_emails` | Sent before 8am, from 7pm or at the weekend. |  |
| `after_hours_share` | after_hours_emails / total_emails. |  |
| `weekend_emails` | Sent on Saturday or Sunday. |  |

## `gold.reply_pairs`

Each reply matched to the email it answers (same subject, reversed sender and recipient, within 14 days, latest candidate).

| Column | Description | PII |
|---|---|---|
| `reply_id` | Message-ID of the reply. |  |
| `original_id` | Message-ID of the email being replied to. |  |
| `replier` | Who sent the reply. | yes |
| `original_sender` | Who sent the original email. | yes |
| `subject_normalised` | Shared normalised subject. |  |
| `original_sent_at` | When the original was sent (UTC). |  |
| `reply_sent_at` | When the reply was sent (UTC). |  |
| `response_minutes` | Minutes between the original and the reply. |  |

## `gold.person_activity`

One row per email address: volume, after-hours load and reply speed.

| Column | Description | PII |
|---|---|---|
| `person` | Email address. | yes |
| `sent_count` | Unique emails sent. |  |
| `after_hours_sent_share` | Share of sent email that was after hours. |  |
| `received_count` | Times the address appears as a To or Cc recipient. |  |
| `replies_matched` | Replies by this person matched in gold.reply_pairs. |  |
| `median_response_minutes` | Median minutes this person took to reply. |  |
| `is_enron` | Address is @enron.com. |  |

## `gold.message_features`

ML feature table: one row per email sent To a mailbox owner, with point-in-time features and the reply-within-2-hours label.

| Column | Description | PII |
|---|---|---|
| `message_id` | Email this row describes. |  |
| `sender` | Sender address (kept for analysis, not used as a feature). | yes |
| `sent_at` | Send time (UTC); used for the time-based train/test split. |  |
| `body_length` | Characters in the cleaned body (quoted text removed). |  |
| `word_count` | Words in the cleaned body. |  |
| `to_count` | Number of To recipients. |  |
| `cc_count` | Number of Cc recipients. |  |
| `recipient_count` | To plus Cc recipients. |  |
| `is_reply` | 1 if the subject starts with Re:. |  |
| `is_forward` | 1 if the subject starts with Fw: or Fwd:. |  |
| `thread_position` | Earlier emails with the same normalised subject (0 = first in thread). |  |
| `has_question` | 1 if the cleaned body contains a question mark. |  |
| `sender_is_internal` | 1 if the sender has an @enron.com address. |  |
| `sender_prior_emails` | Emails the sender had sent before this one. |  |
| `prior_emails_to_recipients` | Most earlier emails from this sender to any one recipient. |  |
| `prior_replies_from_recipients` | Times these recipients had already replied to this sender. |  |
| `hour_sent` | Hour sent, Houston time (0-23). |  |
| `weekday_sent` | Day sent, Houston time (1 = Sunday ... 7 = Saturday). |  |
| `is_after_hours` | 1 if sent before 8am, from 7pm or at the weekend (Houston time). |  |
| `is_meeting` | 1 if the subject or full body matches the meeting keyword rule. |  |
| `has_urgent` | 1 if the subject or body contains 'urgent'. |  |
| `has_asap` | 1 if the subject or body contains 'asap' or 'as soon as possible'. |  |
| `has_deadline` | 1 if it mentions a deadline or 'by today/tomorrow/end of day'. |  |
| `has_action_request` | 1 if it asks the reader to respond, confirm, review, call etc. |  |
| `has_important` | 1 if the subject or body contains 'important'. |  |
| `text` | Subject plus cleaned body; TF-IDF is fitted on this at training time. | yes |
| `label_replied_2h` | Label: 1 if any recipient replied within 120 minutes. |  |

## `gold.model_metrics`

Test-set results of each training run, for the model and the baselines.

| Column | Description | PII |
|---|---|---|
| `run_id` | MLflow run ID. |  |
| `test_from` | Start of the test period. |  |
| `model` | model, model_at_0.5 or a baseline name. |  |
| `threshold` | Probability cut-off used. |  |
| `precision` | Precision for the replied-within-2h class. |  |
| `recall` | Recall for the replied-within-2h class. |  |
| `f1` | F1 for the replied-within-2h class. |  |
| `positives_predicted` | Emails flagged as likely to get a fast reply. |  |
| `test_pr_auc` | Area under the precision-recall curve (model rows only). |  |
| `test_roc_auc` | Area under the ROC curve (model rows only). |  |
| `test_rows` | Emails in the test period. |  |
| `test_positive_rate` | Share of test emails replied to within 2 hours. |  |
| `trained_at` | When the run finished. |  |
