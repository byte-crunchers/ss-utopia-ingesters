-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema bytecrunchers
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema bytecrunchers
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `bytecrunchers` DEFAULT CHARACTER SET utf8 ;
USE `bytecrunchers` ;

-- -----------------------------------------------------
-- Table `bytecrunchers`.`users`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`users` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`users` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `username` VARCHAR(50) NOT NULL,
  `email` VARCHAR(50) NOT NULL,
  `password` CHAR(60) NOT NULL,
  `first_name` VARCHAR(50) NOT NULL,
  `last_name` VARCHAR(50) NOT NULL,
  `is_admin` TINYINT NOT NULL,
  `active` TINYINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `username_UNIQUE` (`username` ASC) VISIBLE,
  UNIQUE INDEX `users_id_UNIQUE` (`id` ASC) VISIBLE,
  UNIQUE INDEX `email_UNIQUE` (`email` ASC) VISIBLE);


-- -----------------------------------------------------
-- Table `bytecrunchers`.`account_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`account_types` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`account_types` (
  `id` VARCHAR(45) NOT NULL,
  `savings_interest` DECIMAL(5,5) UNSIGNED NOT NULL,
  `annual_fee` DECIMAL(6,2) UNSIGNED NOT NULL,
  `foodie_pts` DECIMAL(4,4) UNSIGNED NOT NULL,
  `cashback` DECIMAL(4,4) UNSIGNED NOT NULL,
  `late_fee` DECIMAL(5,2) UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `account_type_id_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bytecrunchers`.`accounts`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`accounts` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`accounts` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `users_id` INT UNSIGNED NOT NULL,
  `account_type` VARCHAR(45) NOT NULL,
  `balance` DECIMAL(10,2) NOT NULL,
  `payment_due` DECIMAL(10,2) NOT NULL,
  `due_date` DATE NULL,
  `credit_limit` INT NULL,
  `debt_interest` DECIMAL(5,5) UNSIGNED NOT NULL,
  `active` TINYINT NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `accounts_users_id_idx` (`users_id` ASC) VISIBLE,
  UNIQUE INDEX `accounts_id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `account_type_idx` (`account_type` ASC) VISIBLE,
  CONSTRAINT `users_id_ac`
    FOREIGN KEY (`users_id`)
    REFERENCES `bytecrunchers`.`users` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `account_type`
    FOREIGN KEY (`account_type`)
    REFERENCES `bytecrunchers`.`account_types` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bytecrunchers`.`transactions`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`transactions` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`transactions` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `origin_account` INT UNSIGNED NOT NULL,
  `destination_account` INT UNSIGNED NOT NULL,
  `memo` VARCHAR(55) NOT NULL,
  `transfer_value` DECIMAL(10,2) NOT NULL,
  `time_stamp` DATETIME NOT NULL,
  INDEX `origin_accounts_id_idx` (`origin_account` ASC) VISIBLE,
  INDEX `destinationaccounts_id_idx` (`destination_account` ASC) VISIBLE,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `transactions_id_UNIQUE` (`id` ASC) VISIBLE,
  CONSTRAINT `origin_accounts_id`
    FOREIGN KEY (`origin_account`)
    REFERENCES `bytecrunchers`.`accounts` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `destinationaccounts_id`
    FOREIGN KEY (`destination_account`)
    REFERENCES `bytecrunchers`.`accounts` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bytecrunchers`.`cards`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`cards` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`cards` (
  `accounts_id` INT UNSIGNED NOT NULL,
  `card_num` BIGINT UNSIGNED NOT NULL,
  `pin` SMALLINT UNSIGNED NULL,
  `cvc1` SMALLINT UNSIGNED NULL,
  `cvc2` SMALLINT UNSIGNED NULL,
  `exp_date` DATE NOT NULL,
  UNIQUE INDEX `cards_accounts_id_UNIQUE` (`accounts_id` ASC) VISIBLE,
  UNIQUE INDEX `card_num_UNIQUE` (`card_num` ASC) VISIBLE,
  PRIMARY KEY (`card_num`),
  CONSTRAINT `card_account`
    FOREIGN KEY (`accounts_id`)
    REFERENCES `bytecrunchers`.`accounts` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bytecrunchers`.`loan_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`loan_types` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`loan_types` (
  `id` VARCHAR(45) NOT NULL,
  `upper_range` DECIMAL(4,4) UNSIGNED NOT NULL,
  `lower_range` DECIMAL(4,4) UNSIGNED NOT NULL,
  `late_fee` DECIMAL(6,2) UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `loan_type_id_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bytecrunchers`.`loans`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`loans` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`loans` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `users_id` INT UNSIGNED NOT NULL,
  `balance` DECIMAL(10,2) NOT NULL,
  `interest_rate` DECIMAL(5,5) NOT NULL,
  `due_date` DATETIME NOT NULL,
  `payment_due` DECIMAL(8,2) UNSIGNED NOT NULL,
  `loan_type` VARCHAR(45) NOT NULL,
  `monthly_payment` DECIMAL(7,2) UNSIGNED NOT NULL,
  `active` TINYINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `loans_id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `user_id_idx` (`users_id` ASC) VISIBLE,
  INDEX `loan_type_idx` (`loan_type` ASC) VISIBLE,
  CONSTRAINT `user_id_l`
    FOREIGN KEY (`users_id`)
    REFERENCES `bytecrunchers`.`users` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `loan_type`
    FOREIGN KEY (`loan_type`)
    REFERENCES `bytecrunchers`.`loan_types` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bytecrunchers`.`branches`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`branches` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`branches` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `location` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `branches_id_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bytecrunchers`.`appointments`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`appointments` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`appointments` (
  `users_id_ap` INT UNSIGNED NOT NULL,
  `branches_id` INT UNSIGNED NOT NULL,
  `start_time` DATETIME NOT NULL,
  `end_time` DATETIME NOT NULL,
  INDEX `banches_id_idx` (`branches_id` ASC) VISIBLE,
  INDEX `appointments_users_id_idx` (`users_id_ap` ASC) VISIBLE,
  CONSTRAINT `users_id_ap`
    FOREIGN KEY (`users_id_ap`)
    REFERENCES `bytecrunchers`.`users` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `banches_id`
    FOREIGN KEY (`branches_id`)
    REFERENCES `bytecrunchers`.`branches` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bytecrunchers`.`card_transactions`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`card_transactions` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`card_transactions` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `card_num` BIGINT UNSIGNED NOT NULL,
  `merchant_account_id` INT UNSIGNED NOT NULL,
  `memo` VARCHAR(55) NOT NULL,
  `transfer_value` DECIMAL(10,2) NOT NULL,
  `pin` SMALLINT UNSIGNED NULL,
  `cvc1` SMALLINT UNSIGNED NULL,
  `cvc2` SMALLINT UNSIGNED NULL,
  `location` CHAR(2) NULL,
  `time_stamp` DATETIME NOT NULL,
  INDEX `destinationaccounts_id_idx` (`merchant_account_id` ASC) VISIBLE,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `card_transactions_id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `card_id_idx` (`card_num` ASC) VISIBLE,
  CONSTRAINT `merchant_account`
    FOREIGN KEY (`merchant_account_id`)
    REFERENCES `bytecrunchers`.`accounts` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `card_id`
    FOREIGN KEY (`card_num`)
    REFERENCES `bytecrunchers`.`cards` (`card_num`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bytecrunchers`.`loan_payments`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bytecrunchers`.`loan_payments` ;

CREATE TABLE IF NOT EXISTS `bytecrunchers`.`loan_payments` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `loan_id` INT UNSIGNED NOT NULL,
  `account_id` INT UNSIGNED NOT NULL,
  `amount` DECIMAL(10,2) NOT NULL,
  `time_stamp` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `loan_pay_ID_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `fk_loan_payments_loans1_idx` (`loan_id` ASC) VISIBLE,
  INDEX `fk_loan_payments_accounts1_idx` (`account_id` ASC) VISIBLE,
  CONSTRAINT `fk_loan_payments_loans1`
    FOREIGN KEY (`loan_id`)
    REFERENCES `bytecrunchers`.`loans` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_loan_payments_accounts1`
    FOREIGN KEY (`account_id`)
    REFERENCES `bytecrunchers`.`accounts` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
