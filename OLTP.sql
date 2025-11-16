--- ============================================
-- FairMoney OLTP Database Schema  
-- ============================================
CREATE SCHEMA IF NOT EXISTS oltp;

CREATE TABLE oltp.collections (
  collection_id integer NOT NULL DEFAULT nextval('oltp.collections_collection_id_seq'::regclass),
  loan_id integer,
  customer_id integer,
  assigned_to character varying,
  case_status character varying DEFAULT 'OPEN'::character varying,
  assignment_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  resolution_date timestamp without time zone,
  notes text,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT collections_pkey PRIMARY KEY (collection_id),
  CONSTRAINT collections_loan_id_fkey FOREIGN KEY (loan_id) REFERENCES oltp.loans(loan_id),
  CONSTRAINT collections_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES oltp.customers(customer_id)
);
CREATE TABLE oltp.countries (
  country_id integer NOT NULL DEFAULT nextval('oltp.countries_country_id_seq'::regclass),
  country_code character varying NOT NULL UNIQUE,
  country_name character varying NOT NULL,
  currency_code character varying NOT NULL,
  is_active boolean DEFAULT true,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT countries_pkey PRIMARY KEY (country_id)
);
CREATE TABLE oltp.customer_kyc (
  kyc_id integer NOT NULL DEFAULT nextval('oltp.customer_kyc_kyc_id_seq'::regclass),
  customer_id integer,
  document_type character varying NOT NULL,
  document_number character varying,
  verification_status character varying DEFAULT 'PENDING'::character varying,
  verified_at timestamp without time zone,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT customer_kyc_pkey PRIMARY KEY (kyc_id),
  CONSTRAINT customer_kyc_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES oltp.customers(customer_id)
);
CREATE TABLE oltp.customers (
  customer_id integer NOT NULL DEFAULT nextval('oltp.customers_customer_id_seq'::regclass),
  country_id integer,
  phone_number character varying NOT NULL UNIQUE,
  email character varying,
  first_name character varying NOT NULL,
  last_name character varying NOT NULL,
  date_of_birth date,
  registration_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  customer_status character varying DEFAULT 'ACTIVE'::character varying,
  customer_segment character varying,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT customers_pkey PRIMARY KEY (customer_id),
  CONSTRAINT customers_country_id_fkey FOREIGN KEY (country_id) REFERENCES oltp.countries(country_id)
);
CREATE TABLE oltp.emi_schedule (
  schedule_id integer NOT NULL DEFAULT nextval('oltp.emi_schedule_schedule_id_seq'::regclass),
  loan_id integer,
  emi_number integer NOT NULL,
  due_date date NOT NULL,
  emi_amount numeric NOT NULL,
  payment_status character varying DEFAULT 'PENDING'::character varying,
  paid_amount numeric DEFAULT 0,
  payment_date date,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT emi_schedule_pkey PRIMARY KEY (schedule_id),
  CONSTRAINT emi_schedule_loan_id_fkey FOREIGN KEY (loan_id) REFERENCES oltp.loans(loan_id)
);
CREATE TABLE oltp.loan_applications (
  application_id integer NOT NULL DEFAULT nextval('oltp.loan_applications_application_id_seq'::regclass),
  customer_id integer,
  product_id integer,
  application_number character varying NOT NULL UNIQUE,
  requested_amount numeric NOT NULL,
  requested_tenure_days integer NOT NULL,
  application_status character varying DEFAULT 'SUBMITTED'::character varying,
  risk_score numeric,
  submitted_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  processed_at timestamp without time zone,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT loan_applications_pkey PRIMARY KEY (application_id),
  CONSTRAINT loan_applications_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES oltp.customers(customer_id),
  CONSTRAINT loan_applications_product_id_fkey FOREIGN KEY (product_id) REFERENCES oltp.loan_products(product_id)
);
CREATE TABLE oltp.loan_products (
  product_id integer NOT NULL DEFAULT nextval('oltp.loan_products_product_id_seq'::regclass),
  country_id integer,
  product_name character varying NOT NULL,
  product_code character varying NOT NULL UNIQUE,
  min_amount numeric NOT NULL,
  max_amount numeric NOT NULL,
  min_tenure_days integer NOT NULL,
  max_tenure_days integer NOT NULL,
  interest_rate numeric NOT NULL,
  processing_fee_rate numeric DEFAULT 0,
  is_active boolean DEFAULT true,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT loan_products_pkey PRIMARY KEY (product_id),
  CONSTRAINT loan_products_country_id_fkey FOREIGN KEY (country_id) REFERENCES oltp.countries(country_id)
);
CREATE TABLE oltp.loans (
  loan_id integer NOT NULL DEFAULT nextval('oltp.loans_loan_id_seq'::regclass),
  customer_id integer,
  application_id integer,
  loan_number character varying NOT NULL UNIQUE,
  principal_amount numeric NOT NULL,
  interest_amount numeric NOT NULL,
  total_amount numeric NOT NULL,
  tenure_days integer NOT NULL,
  emi_amount numeric NOT NULL,
  disbursement_date date,
  maturity_date date,
  loan_status character varying DEFAULT 'ACTIVE'::character varying,
  outstanding_amount numeric,
  days_past_due integer DEFAULT 0,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT loans_pkey PRIMARY KEY (loan_id),
  CONSTRAINT loans_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES oltp.customers(customer_id),
  CONSTRAINT loans_application_id_fkey FOREIGN KEY (application_id) REFERENCES oltp.loan_applications(application_id)
);
CREATE TABLE oltp.payments (
  payment_id integer NOT NULL DEFAULT nextval('oltp.payments_payment_id_seq'::regclass),
  customer_id integer,
  loan_id integer,
  payment_reference character varying NOT NULL UNIQUE,
  payment_method character varying NOT NULL,
  amount numeric NOT NULL,
  payment_type character varying NOT NULL,
  payment_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  payment_status character varying DEFAULT 'COMPLETED'::character varying,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT payments_pkey PRIMARY KEY (payment_id),
  CONSTRAINT payments_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES oltp.customers(customer_id),
  CONSTRAINT payments_loan_id_fkey FOREIGN KEY (loan_id) REFERENCES oltp.loans(loan_id)
);
