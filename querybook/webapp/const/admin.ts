import type { TemplatedForm } from 'ui/SmartForm/SmartForm';

export interface IAdminAnnouncement {
    id: number;
    created_at: number;
    updated_at: number;
    message: string;
    uid: number;
    url_regex: string;
    can_dismiss: boolean;
    active_from?: number;
    active_till?: number;
}

export interface IAdminApiAccessToken {
    id: number;
    created_at: number;
    updated_at: number;
    creator_uid: number;
    updater_uid: number;
    enabled: boolean;
}

export interface IAdminEnvironment {
    id: number;
    name: string;
    description: string;
    image: string;
    public: boolean;
    hidden: boolean;
    shareable: boolean;
    deleted_at: number;
}

export interface IAdminQueryEngine {
    id: number;
    created_at: number;
    updated_at: number;
    deleted_at: number;
    name: string;
    language: string;
    description: string;

    metastore_id: number;
    executor: string;
    executor_params: Record<string, any>;
    feature_params: {
        status_checker?: string;
        upload_exporter?: string;
    };

    environments?: IAdminEnvironment[];
    /**
     * Optional secondary DSN (PostgreSQL only). When set, schedules can opt
     * specific cells into running on this connection instead of the sandbox
     * one via `kwargs.run_on_main_engine_ids`. For env-managed engines the
     * value is masked to "***".
     */
    main_connection_string?: string | null;
    is_env_managed?: boolean;
}

export interface IAdminACLControl {
    type?: 'denylist' | 'allowlist';
    tables?: string[];
}

export interface IAdminMetastore {
    id: number;
    created_at: number;
    updated_at: number;
    deleted_at: number;
    name: string;
    metastore_params: Record<string, unknown>;
    loader: string;
    acl_control: IAdminACLControl;
}

export interface IMetastoreLoader {
    name: string;
    template: TemplatedForm;
}

export interface IQueryEngineTemplate {
    language: string;
    name: string;
    template: TemplatedForm;
}

export interface IAdminUserRole {
    id: number;
    uid: number;
    role: number;
}
