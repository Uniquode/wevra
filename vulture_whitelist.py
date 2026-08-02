# ruff: noqa: B018
# Vulture whitelist: Wybra framework entry points discovered by integrations.


class _Whitelist:
    pass


_ = _Whitelist()

# Package and public helper entry points.
_.__getattr__
_.bootstrap_initial_admin
_.load_composed_settings
_.env_setting_is_set
_.migration_script_root
_.discover_model_metadata
_.redact_database_url
_.model_packages_from_modules
_.migration_version_locations_from_modules
_.migration_version_location_for_configured_module
_.register_error_handlers
_.optional_current_user
_.require_current_user
_.require_anonymous_user
_.resolve_provider_client_secret
_.evaluate_login
_.evaluate_linking
_.login_required
_.anonymous_required
_.has_module
_.has_capability
_.require_capability
_.optional_capability
_.provide_capability
_.capability_proxy
_.start_site
_.get_site
_.setup_site
_.to_json
_.report_progress
_.finalised
_.unavailable
_.template_context_middleware
_.register_cache_key_normaliser
_.diagnostics_middleware
_.session_cookie_cleanup_middleware
_.wybra_messages_middleware
_.wybra_session_middleware
_.diagnostics_websocket
_.wybra_test_client
_._clear_wybra_test_database
_._before_cursor_execute
_._after_cursor_execute
_.set_result
_.is_result_ready
_.get_result
_.session
_.transaction
_.close
_.available
_.require
_.path_for
_.url_for
_.store
_.get_by_resource_key
_.assign_resource_key
_.apply
_.validate_writable
_.get_profile
_.ensure_profile
_.set_profile_picture
_.save_profile_fields
_.save_phone_contact
_.list_phone_contacts
_.recovery_eligible_phone_contacts
_.profile_image_for_user
_.status_response
_.streaming_response
_.page_link
_.Http400
_.Http401
_.Http403
_.Http404
_.development_error_context
_._handle_http_exception
_._handle_validation_error
_._handle_unexpected_exception

# Click command callbacks registered through decorators.
_.create_command
_.update_command
_.delete_command
_.deactivate_command
_.list_command
_.password_command
_.init_command
_.destroy_command
_.tasks_command
_.run_command
_.revision_command
_.makemigrations_command
_.migrate_command_command
_.heads_command
_.sqlmigrate_command
_.set_command
_.rotate_system_command
_.rotate_csrf_command
_.reencrypt_command
_.upgrade_command
_.downgrade_command
_.current_command
_.history_command
_.scope_create_command
_.scope_update_command
_.scope_delete_command
_.scope_list_command
_.group_command

# Auth schemas and authentication extension hooks.
_.UserRead
_.UserUpdate
_.model_config
_._normalise_email
_._normalise_optional_email
_.validate_password
_.on_after_forgot_password
_.on_after_request_verify
_.AdvancedAuthenticationPolicy
_.NoChallengePolicy
_.PRIMARY_ASSERTION_METHOD
_.WEBAUTHN_ASSERTION_METHOD
_.after_primary_authentication
_.available_methods
_.requires_challenge
_.complete_challenge

# Tortoise router hooks discovered from the configured router type.
_.db_for_read
_.db_for_write

# Jinja extension callbacks invoked through Extension.call_method().
_._render_fragment

# Generic view members selected dynamically by HTTP dispatch and form generation.
_.patch
_._generated_form
_.replace_collector_scopes

# Protocol, dataclass, and ORM fields used by consumers and SQLAlchemy.
_.__call__
_.additive_route_names
_.replaces
_.hashed_password
_.kind
_.occurred_at
_.succeeded
_.generate
_.create_challenge
_.ExternalIdentityLinkRecord
_.IdentityEmailStore
_.list_user_emails
_.verify_matching_email
_.ExternalIdentityStore
_.list_user_providers
_.InitialAdminBootstrapStore
_.claim_initial_admin_bootstrap
_.AuthorisationStore
_.TOTPCredentialStore
_.create_pending_totp_credential
_.activate_totp_credential
_.disable_totp_credential
_.get_user_totp_credentials
_.WebAuthnCredentialStore
_.store_webauthn_credential
_.credential_id
_.public_key
_.sign_count
_.update_webauthn_sign_count
_.RecoveryCodeStore
_.replace_recovery_codes
_.code_hashes
_.consume_recovery_code
_.code
_.destroy_token
_.create_database_strategy
_.create_session_token_strategy
_.delete_session_token_by_value
_.SqlAlchemyProviderCredentialStore
_.create_provider_credential
_.get_provider_credential
_.decrypt_access_token
_.decrypt_refresh_token
_.oauth_accounts
_.token_secrets_configured
_.integration_enabled
_.provider_subject
_.crypt_access_token
_.crypt_refresh_token
_.crypt_secret
_.last_used_counter
_.status
_.health
_.activated_at
_.disabled_at
_.totp_credentials
_.webauthn_credentials
_.account_email
_.provider_enabled
_.provider_metadata
_.discovery_document_url
_.token_type
_.raw
_.links
_.emails
_.provider_id
_.user_id
_.external_identity_links
_.provider_name
_.is_primary
_.encrypt_required
_.decrypt_required
_.refresh_key_ring
_.from_env
_.for_testing
_.from_key_bundle
_.generate_totp
_.totp_recovery_window_expiry_timestamp
_._normalise_identity_email
_.SCOPE_RECORD_FIELDS
_.primary_table_name
_.list_candidate_child_groups_for_management
_.RESERVED_TEMPLATE_CONTEXT_KEYS
_.shadowed
_.totp_enabled
_.passkey_enabled
_.include_prefix
_.accepts_body
_.accepts_form
_.path_parameters
_.template
_.middleware_stack
_.src
_.alt
_.fallback_text
_.authenticated
_.compare_and_swap
_.compare_and_delete
_.increment
_.acquire
_.renew
_.reserve
_.reject
_.dead_letter
_.read_consumer
_.forget_consumer
_.due
_.durable
_.restart_recovery
_.horizontal_consumers
_.guarantees

# Typed event fields selected by the diagnostics projection through dataclass
# reflection, plus an ASGI middleware callback registered by FastAPI's decorator.
_.model_type
_.key_fingerprint
_.field_count
_.invalid_field_count
_.model_types
_.stale_conflict
_.masked_email
_.mechanism
_.event_lifecycle_middleware
_.profile_image
_.profile_path
_.settings_menu

# Generic view metadata accessed by the topic-owned event descriptor.
_._content_type_identifier
_._model_type_identity

# Declarative form fields collected from class namespaces by Form.
_.bypass_totp_setup
_.setup_totp_code
_.confirmation
_.root
_.mount_path
_.serve
_.profile_picture_media_id
_.preferred_name
_.display_name
_.bio
_.first_name
_.last_name
_.pronouns
_.phone_number
_.website_links
_.country_region
_.profile_link_website
_.phone_subdivision_code
_.city
_.postal_code
_.job_title
_.company
_.company_industry
_.department
_.date_time_format
_.theme
_.notification_preferences
_.profile_visibility
_.marketing_consent
_.terms_accepted_at
_.data_deletion_requested
_.country_code
_.subdivision_code
_.normalised_number
_.number_type
_.sms_capable
_.verified_at
_.dial_prefix
_.flag
_.max_length
_.pronoun_options
_.website
_.model
_.settings_path
_.dispatch
_.maintenance_tasks
_.recommended_frequency

# SQLAlchemy TypeDecorator hooks and fields.
_.impl
_.cache_ok
_.load_dialect_impl
_.process_bind_param
_.process_result_value
_.filename
_.exception_message
_.traceback
_.htmx
_.committed
_.consume_alerts
_.accessed
_.mark_accessed
_.normalised_phone_contact
_.display_label
_.icon_token
_.shortcut
_.css_class
_.layout
_.spacing
_.alignment
_.item_alignment
_.shortcut_scope
_.id

# Alembic revision module metadata.
_.initial
_.down_revision
_.branch_labels
_.depends_on

# Tortoise model metadata and fields consumed by Tortoise at runtime.
_.unique_together
_.indexes
_.consumed_at
_.parent_group_id
_.child_group_id
_.parent_group
_.child_group
_.profile_picture_media

# HTMLParser callback invoked by the standard library.
_.handle_starttag

# Taskiq broker and scheduler hooks invoked by its worker runtime.
_.kick
_.listen
_.add_schedule
_.delete_schedule
_.get_schedules
_.pre_send
_.post_send
