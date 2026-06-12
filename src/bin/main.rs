use std::rc::Rc;

use s3_sink::*;
use tracing::{error, info};

fn get_env_var_or_panic(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("could not get env var '{name}'"))
}

fn get_env_var_or_default(name: &str, default: impl FnOnce() -> String) -> String {
    std::env::var(name).unwrap_or_else(|_| default())
}

fn get_u64_env_var_or_panic(name: &str) -> u64 {
    get_env_var_or_panic(name)
        .parse::<u64>()
        .unwrap_or_else(|_| panic!("could not get env var '{name}' as u64"))
}

fn get_config() -> SinkConfig {
    // temporary until we set-up a dynamic way to inject it
    let input_topics = {
        let topics: Vec<&str> = (if get_env_var_or_panic("ENV") == "prod" {
            "AccountDeletionComplete,AccountDeletionFailed,AccountDeletionRequested,AdyenDispute,AdyenExternalSettlementDetail,AdyenPaymentsAccounting,AdyenReceivedPayments,AdyenSettlementDetail,AmazonConnectAgentEvents,AmazonConnectContactTraceRecords,AppAnnieDownloads,AppAnnieUsage,AppAnnieUsageMonthly,AtocInventoryChangeOperation,AtocInventoryProduct,AtocInventoryProductDeliverablesUpdated,AtocInventoryProductIssued,AtocRailcardInventoryProduct,AuthenticationChallenged,BarclaysAccountingEntries106,BarclaysCurrencyEntries,BarclaysCurrencyTotals,BarclaysTotalsRecord,BasketPriced,BasketValidated,BebocAfterSalesCharges,BebocAfterSalesIntents,BebocCards,BebocChannels,BebocConditionDescriptions,BebocConditions2,BebocCreditCards,BebocDiscounts,BebocFolders,BebocInternalPayments,BebocInvoiceLines,BebocInvoices,BebocOrders,BebocPassengers,BebocPayments,BebocPnrGroups,BebocPnrHeaders,BebocPnrs,BebocRefunds,BebocSegmentOptions,BebocSegments,BebocStations,BebocSubscriptions,BebocTickets,BebocTravelers,BebocTravels,BebocTrips,BebocUsers,BenerailManualDataFeeds,BookerCustomFieldsUpdated,BookingAPICars,BookingAPIDataFeeds,BookingAPIOrders,BotifyGoogleTrains,Braintree,BrainTrust,Branch.io.events.eo_commerce_event,Branch.io.installs.eo_install,Branch.io.installs.eo_reinstall,Branch.io.opens.eo_open,BranchCustomExportsEO,BranchCustomExportsSKAdNetwork,BrazeNewAdvanceChangeToJourneyDepartureOrArrivalTimeNotification,BrazeNewImmediateDisruptionNotification,Brightcom,BusinessCardQualifiersUpdated,BusinessCardUpdated,BusinessSettingUpdate,CacheSeederPriceComparison,CallingPointDeparturePlatformChanged,CarrierCallback,CarrierCallbackCompensationFailed,CarrierCallbackCompensationSucceeded,CarTrawlerDailyTransactions,ChallengedAuthenticationAccepted,ChallengedAuthenticationCompleted,ChallengedAuthenticationFailed,ChallengedAuthenticationRejected,ChallengedAuthenticationRequested,ChangeOfJourneyEligibilityRequest,ChangeOfJourneyEligibilityResult,ChattermillTrainlinemain,CoJEligibilityRequested,CommuteCreatedOrUpdated,CommuteDeleted,CommuteRead,CompanjonMFCN,CompanjonRTPSN,CompensatingActionsComplete,CompensatingActionsStarted,CompensationCompleted,CompensationFailed,CompensationRejected,CompensationRequested,CompensationUpdated,ConnectionHealth,ConsentService,ContactCentreAgent,ContactCentreAgentTelephonyAccountChanged,ContactCentreAgentTelephonyAccountChangeFailed,ContactCentreCompensationRequestApproved,ContactCentreCompensationRequestCreated,ContactCentreCompensationRequestRejected,ContactCentreCustomerDetailsUpdated,ContactCentreDiscretionaryRefundRequestApproved,ContactCentreDiscretionaryRefundRequestCreated,ContactCentreDiscretionaryRefundRequestRejected,ContactCentreEmailResendRequested,ContactCentreEuExceptionalRefundStatusUpdated,ContactCentreExceptionalRefundQuoteConfirmedEvent,ContactCentreFlexiSeasonCancel,ContactCentreFraudCheckBulkUploaded,ContactCentreFulfilmentConverted,ContactCentreLogin,ContactCentreLoginFailed,ContactCentreMecClaimUpdated,ContactCentreMecReportGenerated,ContactCentreNotes,ContactCentreOrderLoaded,ContactCentrePlatformOrderCompensationRequestActioned,ContactCentrePlatformOrderCompensationRequestCreated,ContactCentreRefreshBookingRequest,ContactCentreRefundQuoteConfirmedEvent,ContactCentreReplaceBookingRequest,ContextAppended,ContextClassifierDefaultCurrencyClassified,ContextClassifierWebLoyaltyClassified,ContextCreated,Corporate,CorporateSignUpAgreementAccepted,CorporateSsoConfigurationUpdated,CorporateSynced,CorporateTravellerProfile,CorporateUpdate,CorporateUpdated,CoverGeniusMonthlyFeeds,CreateReservationExecuted,CreditCreated,CreditDetailsCreated,CreditIssued,CreditIssuing,CreditIssuingFailed,CROuigoEDBTickets,CROuigoFranceTickets,CRSNCFEDBTickets,CRSNCFTickets,CurrencyDecision,CustomerAttributeCustomerLocation,CustomerBasketAssociations,CustomerDataDeletionRequest,CustomerEmailAddressUpdate,CustomerOriginClassified,CustomerOriginNotClassified,CustomerOriginPlatformChanged,CustomerServiceAmendedCustomer,CustomerServiceFailedLogin,CustomerServiceGuestRegistration,CustomerServiceLogin,CustomerServicePasswordReset,CustomerServicePasswordResetFailed,CustomerServicePasswordResetRequested,CustomerServicePreferredLanguageSet,CustomerServiceRegistration,CustomerTravelServiceCancelled,CustomerTravelServiceDelay,CustomerTravelServiceReinstated,CustomerTreatment,CustomFieldsRuleCreated,CustomFieldsRuleUpdated,CustomFieldsValueListCreated,CustomFieldsValueListUpdated,CybersourcePaymentBatchDetailReport,CybersourceTransactionRequestDetailReport,DarwinForecast,DarwinSchedule,DeliveryMethodChangeContextCreated,DeliveryOptionsOffered,DeliveryReady,DisruptedCommuteNotification,DisruptedCommutes,DisruptionCreatedOrUpdated,DisruptionDeleted,DisruptionNewImmediateItalianNotification,DisruptionNewRealTimeFullCancellationNotification,DisruptionNewRealTimeReinstatementNotification,DisruptionNewRealTimeStationCancellationNotification,DisruptionRealTimeDelayNotification,DisruptionRegistered,DisruptionUpdateForRouteAndTimeWindow,DocumentReady,DuranceRecommended,DynamicETicketCancellationStateChanged,DynamicETicketCreated,DynamicETicketDeviceBindingCreated,DynamicETicketPassActivation,Elavon,EnvironmentalImpactCalculation,EUFareSearchExecuted,EUInventoryChangeOperation,EUInventoryProduct,EuPreFilterSearchResults,EURailcardInventoryProduct,EuRealtimeCallingPoints,EurostarDirectManualDataFeeds,EvaluationCaptureFeesEngineVortexEvent,EventDestinationModified,ExtAppIosReviews,ExtCurrencyRatesMorningStar,ExtLeanplumAndroidABTests,ExtLeanplumIosABTests,ExtVendorBusbud,ExtVendorDeutschebahnPst,ExtVendorDistribution,ExtVendorEURail,ExtVendorObb,ExtVendorRenfe,ExtVendorSBB,ExtVendorSBBV3,ExtVendorWestbahn,FavouriteLocationCreatedOrUpdated,FavouriteLocationDeleted,FlixbusMonthlyFeeds,FonoaFailingEvent,ForgotPasswordEmail,FreshchatClassic,FreshdeskActivities,FreshdeskTicketProperties,FreshserviceDirectExportApprovals,FreshserviceDirectExportProblems,FreshserviceDirectExportTasks,FreshserviceDirectExportTickets,FrictionlessAuthenticationFailed,FrictionlessAuthenticationRejected,FrictionlessAuthenticationRequested,FrictionlessAuthenticationSucceeded,FulfilmentCompleted,GAMAdRequestsReport,GAMAdUnits,GAMPerformanceReport,GAMRevenueReport,GatewaySearchExecutedEvent,GeneratedUTN,GetYourGuide,GetYourGuideInvoices,GlobalPayments,GlobalPaymentsDeposits,GlobalPaymentsListSettledDisputes,GlobalPaymentsSettledTransactions,GlobalPaymentsTC40,GooglePassGeneratedProduct,GrrcnAdjustment,GrrcnChargeback,GrrcnHeader,GrrcnSubmission,GrrcnSummary,GrrcnTrailer,GrrcnTransaction,Ilsa,InformationRetrievalApi,InsuranceClaimEligibilityChecked,InsuranceClaimEligibilityCheckFailed,InsuranceProductClaimed,InsuranceProductClaimFailed,InsuranceProductCreated,InsuranceProductInsurantsUpdated,InsuranceProductIssued,InsuranceProductLocked,InsuranceProductsRecommended,InsuranceProductVoidContextCreated,InsuranceProductVoidContextFailed,InsuranceProductVoidContextLocked,InsuranceProductVoidContextVoided,InsuranceProductVoided,InsuranceQuoteCreated,IntegratorUpdate,InventoryInvoiceCreated,InventoryInvoiceGenerated,Iryo,ItineraryCustomFieldsUpdated,ItineraryGenerated,ItineraryRegistrationEvent,JourneyCombinerModelAPI,LegSeatChangeUkNotification,LennonAdjustments,LennonCommission,LennonEarnings,LennonIssuerFees,LennonJvIssuerFees,LennonPrivateSettlement,LennonSales,LicenseNodeCreated,LicenseNodeDeleted,LicenseNodeUpdated,LicenseStructureDeleted,LodgeCardExtractSent,LodgeCardSelected,ManagedGroupUpdate,MarginsUpdated,MintyJourneySearchResponse,MTicketStatusChange,NationalExpressProduct,NetworkTokenCryptogramRetrieved,NetworkTokenDeactivated,NetworkTokenProvisioned,NetworkTokenProvisionFailed,NetworkTokenProvisionRequested,NetworkTokenUpdated,NotificationDataCreated,NrsRequestEvent,NtvSalesManualDataFeeds,NullPrintingFailed,NullPrintSdciEvent,NxFareSearchExecuted,NxVoidableEvent,NxVoidContextEvent,OmnipayFundingAccount,OmnipayInterchangeDetails,OmnipayTransactions,OnHoldItineraryCreated,OnHoldItineraryDeleted,OpenLineageFrostboltMetrics,OrarioTreniEvents,OrarioTreniIosReviews,Order,OrderCustomerUpdated,OrderItemsChangeRequest,OrderNotificationFailureEvent,OrderNotificationSuccessEvent,OrderVatCalculatedEvent,OrganisationalUnitUpdate,OverrideLastDayOfUsage,PartnerPriceMiss,PartnerPriceReconciliation,PartnerPriceSent,PartnerProduct,PartnerSessionCreatedEvent,PassengerCustomFieldsUpdated,PassengerInformationSubmitted,PassengerServicePassengersAddedToAccountHolder,PassengerServicePassengersDeleted,PassengerServicePassengersUpdated,PAYGDailyCharge,PAYGDidNotTravelDisputeReceived,PAYGDisputeCreated,PAYGDisputeReceived,PAYGDisputeResolved,PAYGDisputeServiceNotFound,PAYGFinalJourneyDeterminationCreated,PAYGFraudRiskEvent,PAYGJourneyAddedToCapProduct,PAYGJourneyAddedToSeasonProduct,PAYGJourneyTriageFlagged,PAYGOnboardingFailed,PAYGOnboardingSucceeded,PAYGOrderAssociatedToLedger,PAYGProductIdentified,PAYGPushNotificationSent,PAYGStationsDisputeReceived,PAYGTrackingSessionStarted,PAYGTrackingSessionStopped,PaymentAuthorisationRequested,PaymentAuthorisationReversed,PaymentAuthorised,PaymentCaptured,PaymentCaptureRequested,PaymentCardInformationAcquired,PaymentCreated,PaymentDetailsCreated,PaymentFailed,PaymentFeeOfferGenerated,PaymentOffersGenerated,PaymentProviderRecommended,PaymentRefundCreated,PaymentRefunded,PaymentRefundFailed,PaymentRefundRejected,PaymentRejected,PaymentReverseAuthorisationRequested,PaypalDDRReport,PaypalSTLReport,PaypalTRRReport,PdfTicketEmail,PigmentTPSRevenue,Portal25kvTLEUSearch,Portal25kvVendorCallsStatistics,PostIssueDeliveryStateUpdate,PreDepartureJourneyNotification,PreDepartureUkFirstLegNotification,PreDepartureUkSubsequentLegNotification,PriceCacheStalenessPrediction,PricePrediction,PrivacyServiceGetConsent,PrivacyServiceGetConsentAttemptFailed,PrivacyServiceMobileGuestOptedinFromOptOut,PrivacyServiceSetConsent,PrivacyServiceSetConsentAttemptFailed,ProductDelayNotification,ProductFulfilmentTechnicalVoidFailed,ProductModifiedExternallyResult,ProductNotIssuable,ProductProtocolProductSuperseded,ProductVoidResult,ProfileCustomFieldsUpdated,ProfileSyncBulkUploadBatchCompleted,PromocodeCreated,PromocodeRedeemed,PromocodeReinstated,PromocodeValidated,PushNotificationGenerated,RaildataEventsPlanningLookahead,RatesUpdated,RavelinBulkUploadCustomerTagUpdateSucceeded,RavelinCustomerLoginResponse,RavelinRequest,RavelinResponse,RebookingPredictionCustomer,RebookRefundContext,RebookRefundContextConfirmRefundFailed,RefundableFailed,RefundableSucceeded,RefundAbuseChecked,RefundCompleted,RefundEligibility,RefundEmail,RefundInvoiceNumberUpdated,RefundQuoteAmountOverridden,RefundQuoteCreated,RefundQuoteCreationFailed,RefundQuoteCreditDetailsAdded,RefundQuoteRefundFeesAdded,RefundQuoteRefundFeesRemoved,RefundQuoteUnprocessableStatusOverridden,RefundReferredForSuspectedAbuse,RefundRequested,RefundSuccessfulNotificationSuccess,RefundTimedOut,RegistrationConfirmationEmail,ReleaseProduct,ReservationFailure,ReturnOfMoneyAttempt,ReturnOfMoneyFailed,ReturnOfMoneySucceeded,RiskAssessmentCompleted,RuleSetProviderFeesEngineVortexEvent,SatispayPaymentFeeReports,SavedPassengerDetails,SDCIRecord2C,SDCIRecordBE,SDCIRecordBF,SDCIRecordBM,SDCIRecordBN,SDCIRecordBP,SDCIRecordBR,SDCIRecordBS,SDCIRecordCF,SDCIRecordCG,SDCIRecordDB,SDCIRecordDD,SdciRefundEvent,SDCIShift,SDCITrumpsShifts,SDCITrumpsTickets,SearchMcpServerEvent,SearchPredictionAPI,SeasonFaresResponse,SeasonsFRT,SecureTravelDocumentInteraction,Sendgrid,SensorTowerActiveUsers,SensorTowerDownloadsBySource,SignalBoxDetectInteraction,SimilarWeb,SimilarWebSegments,SmartCardData,SmartcardReplacement,SmartExperienceTreatment,SNCFGTFSRealtimeServiceAlerts,SNCFGTFSRealtimeTripUpdates,SNCFGTFSStaticAgency,SNCFGTFSStaticCalendarDates,SNCFGTFSStaticFeedInfo,SNCFGTFSStaticRoutes,SNCFGTFSStaticStops,SNCFGTFSStaticStopTimes,SNCFGTFSStaticTransfers,SNCFGTFSStaticTrips,SNCFIntercityRegularity,SNCFTarifsIntercities,SNCFTERRegularity,SNCFTGVAxes,SNCFTGVRegularity,SNCFTransilienRegularity,SNCFTRRM,SplitTicketRecommenderResponse,SustainabilityFlightData,T25kvAccessGroups,T25kvAfterSalesCharges,T25kvAfterSalesIntents,T25kvCapturedValues,T25kvChannels,T25kvConditionDescriptions,T25kvConditions2,T25kvCoupons,T25kvDiscounts,T25kvFolders,T25kvGhostCards,T25kvHoldings,T25kvInfoDefinitions,T25kvInquiries,T25kvInquiriesPayables,T25kvInternalPayments,T25kvInvoiceLines,T25kvInvoices,T25kvMemberships,T25kvOrders,T25kvOrganizations,T25kvPassengers,T25kvPayments,T25kvPlanBills,T25kvPNRGroups,T25kvPNRHeaders,T25kvPNRS,T25kvRefunds,T25kvSegmentOptions,T25kvSegments,T25kvSources,T25kvStations,T25kvSubscriptions,T25kvTickets,T25kvTravelers,T25kvTravels,T25kvTrips,T25kvUsers,ThreeDSecureSessionCreated,ThreedsExemptionRecommender,TicketAlertCustomerNotification,TicketAlertSubscriptionCreateRequested,TicketBatchFinder,TicketKeeper,TicketKeeperProxyRequestEvent,TicketScanEvent,TLEUSearch,TokenisedCardNumbersDeleted,TpsSalesforceAccount,TpsSalesforceOpportunity,TracsCorporateSecurity,TracsCustomerManagedGroups,TracsCustomerRegistrations,TracsCustomers,TracsStandardRefundArrivalMessage,TracsStandardRefundedMessage,TracsStandardRefundRejectedMessage,TracsStandardRefundRequestMessage,TracsStandardRefundsFRM,TracsStations,TravelAssistantAIFeedbackSubmitted,TravelAssistantChatCreated,TravelAssistantCSFeedbackSubmitted,TravelAssistantHandoverConfirmed,TravelAssistantHandoverOffered,TravelAssistantMessageFeedbackSubmitted,TravelAssistantMessageSent,TravelAssistantOrchestrator,TravelAssistantRefundAgent,TravelAssistantRefundConfirmed,TravelAssistantRefundEligibilityReturned,TravelAssistantRefundQuoteRequested,TravelAssistantRefundQuoteReturned,TravelCardService,TravelDelayRepayBrazeClaimEligible,TravelDelayRepayBrazeClaimNotificationEligible,TravelDelayRepayClaimEligible,TravelDelayRepayClaimExpired,TravelDelayRepayClaimNotificationEligible,TravelDelayRepayClaimStatusChanged,TravelDelayRepayJourneyPosted,TravelDisruptionLLMService,TravelForesightInference,TravelForesightService,TravelInsightsServiceResponse,TravelPolicyCreated,TravelPolicyDeleted,TravelPolicyEvaluationResults,TravelPolicyOverrideItineraryReasonCreated,TravelPolicyUpdated,TravelServiceCancellationStatusChanged,TravelServiceFullyReinstated,TravelServiceRealTimeStatusChanged,TrenItaliaPicoDomestic,TrenItaliaPicoInternational,Ufac-AccessTokenCreated,Ufac-ExchangeTokenCreated,UkCardTypeReferenceDataUpdated,UkCarrierReferenceDataUpdated,UKExtraTypeReferenceDataUpdated,UkFareCategoryReferenceDataUpdated,UkFareDiscountRailReferenceDataUpdated,UKFarePromotionChanged,UKFareSearchExecuted,UkFareTypeReferenceDataUpdated,UKJourneySearchSplitSaveData,UkLocationReferenceDataUpdated,UkPassengerTypeReferenceDataUpdated,UkRouteRestrictionReferenceDataUpdated,UKSeasonFareSearchExecuted,UkTransportModeReferenceDataUpdated,UkTravelClassReferenceDataUpdated,VendorCallDetails,VendorCallsStatistics,VoidableMatchCompleted,VoidableStatusEvent,VoidContextEvent,VoidFailure,VoucherifyCustomer,VoucherifyPublication,VoucherifyRedemption,VoucherifyVoucher,WalkUpBuyRecommenderMlService,WasabiAssignmentEvent,WasabiExperimentAuditEvent,WebloyaltyCommercialRates,WebloyaltyDailyValidJoins,WebloyaltyMonthlyIntlReport,WebloyaltyMonthlyReport,WebloyaltyPDFReport,WebloyaltyWeeklyIntlReport,WebloyaltyWeeklyReport,ZendeskTicketEvents,ZendeskTickets,ZeroAmountAuthorisationRejected,ZeroAmountAuthorisationRequested,ZeroAmountAuthorisationSuccessful,ZyteTrainScraping"
        } else {
            "AccountDeletionComplete,AccountDeletionFailed,AccountDeletionRequested,AdyenExternalSettlementDetail,AtocInventoryChangeOperation,AtocInventoryProduct,AtocInventoryProductDeliverablesUpdated,AtocInventoryProductIssued,AtocRailcardInventoryProduct,AuthenticationChallenged,BasketPriced,BasketValidated,BookerCustomFieldsUpdated,BookingAPIAccommodations,BookingAPICars,BookingAPIFlights,BookingAPIOrders,BrazeNewAdvanceChangeToJourneyDepartureOrArrivalTimeNotification,BrazeNewImmediateDisruptionNotification,BusinessCardQualifiersUpdated,BusinessCardUpdated,BusinessSettingUpdate,CallingPointDeparturePlatformChanged,CarrierCallback,CarrierCallbackCompensationFailed,CarrierCallbackCompensationSucceeded,ChallengedAuthenticationAccepted,ChallengedAuthenticationCompleted,ChallengedAuthenticationFailed,ChallengedAuthenticationRejected,ChallengedAuthenticationRequested,ChangeOfJourneyEligibilityRequest,ChangeOfJourneyEligibilityResult,CoJEligibilityRequested,CommuteCreatedOrUpdated,CommuteDeleted,CommuteRead,CompensatingActionsComplete,CompensatingActionsStarted,CompensationCompleted,CompensationRequested,ConnectionHealth,ConsentService,ContactCentreAgent,ContactCentreCompensationRequestApproved,ContactCentreCompensationRequestCreated,ContactCentreDiscretionaryRefundRequestApproved,ContactCentreDiscretionaryRefundRequestCreated,ContactCentreDiscretionaryRefundRequestRejected,ContactCentreEmailResendRequested,ContactCentreEuExceptionalRefundStatusUpdated,ContactCentreExceptionalRefundQuoteConfirmedEvent,ContactCentreLogin,ContactCentreLoginFailed,ContactCentreMecClaimUpdated,ContactCentreMecReportGenerated,ContactCentreNotes,ContactCentreOrderLoaded,ContactCentreRefreshBookingRequest,ContactCentreRefundQuoteConfirmedEvent,ContactCentreReplaceBookingRequest,ContextAppended,ContextClassifierDefaultCurrencyClassified,ContextClassifierWebLoyaltyClassified,ContextCreated,Corporate,CorporateSignUpAgreementAccepted,CorporateSsoConfigurationUpdated,CorporateSynced,CorporateTravellerProfile,CorporateUpdate,CreateReservationExecuted,CreditCreated,CreditDetailsCreated,CreditIssued,CreditIssuing,CreditIssuingFailed,CurrencyDecision,CustomerAttributeCustomerLocation,CustomerBasketAssociations,CustomerDataDeletionRequest,CustomerEmailAddressUpdate,CustomerOriginClassified,CustomerOriginPlatformChanged,CustomerServiceAmendedCustomer,CustomerServiceFailedLogin,CustomerServiceGuestRegistration,CustomerServiceLogin,CustomerServicePasswordReset,CustomerServicePasswordResetRequested,CustomerServicePreferredLanguageSet,CustomerServiceRegistration,CustomerTravelServiceCancelled,CustomerTravelServiceDelay,CustomerTravelServiceReinstated,CustomerTreatment,CustomFieldsRuleCreated,CustomFieldsRuleUpdated,CustomFieldsValueListCreated,CustomFieldsValueListUpdated,DarwinSchedule,DeliveryOptionsOffered,DeliveryReady,DisruptionCreatedOrUpdated,DisruptionDeleted,DisruptionNewRealTimeFullCancellationNotification,DisruptionNewRealTimeReinstatementNotification,DisruptionNewRealTimeStationCancellationNotification,DisruptionRealTimeDelayNotification,DisruptionRegistered,DocumentReady,DuranceRecommended,DynamicETicketCreated,DynamicETicketDeviceBindingCreated,DynamicETicketPassActivation,EnvironmentalImpactCalculation,EUFareSearch,EUFareSearchExecuted,EUInventoryChangeOperation,EUInventoryProduct,EuPreFilterSearchResults,EURailcardInventoryProduct,EuRealtimeCallingPoints,EvaluationCaptureFeesEngineVortexEvent,ExtVendorEURail,FavouriteLocationCreatedOrUpdated,FonoaFailingEvent,ForgotPasswordEmail,FrictionlessAuthenticationFailed,FrictionlessAuthenticationRejected,FrictionlessAuthenticationRequested,FrictionlessAuthenticationSucceeded,FulfilmentCompleted,GatewaySearchExecutedEvent,GeneratedUTN,GooglePassGeneratedProduct,InformationRetrievalApi,InsuranceProductCreated,InsuranceProductInsurantsUpdated,InsuranceProductIssued,InsuranceProductLocked,InsuranceProductsRecommended,InsuranceProductVoidContextCreated,InsuranceProductVoidContextVoided,InsuranceQuoteCreated,IntegratorUpdate,InventoryInvoiceCreated,InventoryInvoiceGenerated,ItineraryCustomFieldsUpdated,ItineraryRegistrationEvent,JourneyCombinerModelAPI,LegSeatChangeUkNotification,LicenseNodeCreated,LicenseNodeDeleted,LicenseNodeUpdated,LicenseStructureDeleted,LodgeCardExtractSent,LodgeCardSelected,ManagedGroupUpdate,MarginsUpdated,MintyJourneySearchResponse,MTicketStatusChange,NationalExpressProduct,NetworkTokenCryptogramRetrieved,NetworkTokenDeactivated,NetworkTokenProvisioned,NetworkTokenProvisionFailed,NetworkTokenProvisionRequested,NetworkTokenUpdated,NewCustomerPromocode,NotificationDataCreated,NrsRequestEvent,NullPrintingFailed,NullPrintSdciEvent,NxFareSearchExecuted,OnHoldItineraryCreated,OnHoldItineraryDeleted,Order,OrderCustomerUpdated,OrderNotificationFailureEvent,OrderNotificationSuccessEvent,OrderVatCalculatedEvent,OrganisationalUnitUpdate,PartnerPriceReconciliation,PartnerProduct,PartnerSessionCreatedEvent,PassengerCustomFieldsUpdated,PassengerInformationSubmitted,PassengerServicePassengersAddedToAccountHolder,PassengerServicePassengersDeleted,PassengerServicePassengersUpdated,PAYGDailyCharge,PAYGDisputeCreated,PAYGDisputeResolved,PAYGDisputeServiceNotFound,PAYGFinalJourneyDeterminationCreated,PAYGFraudRiskEvent,PAYGOnboardingFailed,PAYGOnboardingSucceeded,PAYGOrderAssociatedToLedger,PAYGProductIdentified,PAYGPushNotificationSent,PAYGTrackingSessionStarted,PAYGTrackingSessionStopped,PaymentAuthorisationRequested,PaymentAuthorisationReversed,PaymentAuthorised,PaymentCaptured,PaymentCaptureRequested,PaymentCardInformationAcquired,PaymentCreated,PaymentDetailsCreated,PaymentFailed,PaymentFeeOfferGenerated,PaymentOffersGenerated,PaymentRefundCreated,PaymentRefunded,PaymentRefundFailed,PaymentRefundRejected,PaymentRejected,PaymentReverseAuthorisationRequested,PdfTicketEmail,PigmentTPSRevenue,PostIssueDeliveryStateUpdate,PreDepartureJourneyNotification,PreDepartureUkFirstLegNotification,PreDepartureUkSubsequentLegNotification,PriceCacheStalenessPrediction,PricePrediction,PrivacyServiceGetConsent,PrivacyServiceGetConsentAttemptFailed,PrivacyServiceMobileGuestOptedinFromOptOut,PrivacyServiceSetConsent,ProductFulfilmentTechnicalVoidFailed,ProductNotIssuable,ProductProtocolProductSuperseded,ProductVoidResult,ProfileCustomFieldsUpdated,ProfileSyncBulkUploadBatchCompleted,PromocodeCreated,PromocodeRedeemed,PromocodeValidated,PushNotificationGenerated,RatesUpdated,RavelinBulkUploadCustomerTagUpdateSucceeded,RavelinCustomerAccountReclaimed,RavelinCustomerLoginResponse,RavelinRequest,RavelinResponse,RebookingPredictionCustomer,RebookRefundContext,RebookRefundContextConfirmRefundFailed,RefundableFailed,RefundableSucceeded,RefundAbuseChecked,RefundCompleted,RefundEligibility,RefundEmail,RefundInvoiceNumberUpdated,RefundQuoteAmountOverridden,RefundQuoteCreated,RefundQuoteCreationFailed,RefundQuoteCreditDetailsAdded,RefundQuotePaidFeesAdded,RefundQuotePaidFeesRemoved,RefundQuoteRefundFeesAdded,RefundQuoteRefundFeesRemoved,RefundReferredForSuspectedAbuse,RefundRequested,RefundSuccessfulNotificationSuccess,RefundTimedOut,RegistrationConfirmationEmail,ReleaseProduct,ReservationFailure,ReturnOfMoneyAttempt,ReturnOfMoneyFailed,ReturnOfMoneySucceeded,RiskAssessmentCompleted,RuleSetProviderFeesEngineVortexEvent,SavedPassengerDetails,SDCIRecord2C,SDCIRecordBE,SDCIRecordBM,SDCIRecordBN,SDCIRecordBP,SDCIRecordBR,SDCIRecordBS,SDCIRecordCF,SDCIRecordCG,SDCIRecordDB,SDCIRecordDD,SdciRefundEvent,SearchMcpServerEvent,SearchPredictionAPI,SecureTravelDocumentInteraction,SignalBoxDetectInteraction,SmartExperienceTreatment,SplitTicketRecommenderResponse,ThreeDSecureSessionCreated,ThreedsExemptionRecommender,TicketKeeperProxyRequestEvent,TicketScanEvent,TokenisedCardNumbersDeleted,TravelAssistantAIFeedbackSubmitted,TravelAssistantChatCreated,TravelAssistantCSFeedbackSubmitted,TravelAssistantHandoverConfirmed,TravelAssistantHandoverOffered,TravelAssistantMessageFeedbackSubmitted,TravelAssistantMessageSent,TravelAssistantOrchestrator,TravelAssistantRefundAgent,TravelAssistantRefundConfirmed,TravelAssistantRefundEligibilityReturned,TravelAssistantRefundQuoteRequested,TravelAssistantRefundQuoteReturned,TravelDelayRepayBrazeClaimEligible,TravelDelayRepayClaimEligible,TravelDelayRepayClaimExpired,TravelDelayRepayClaimStatusChanged,TravelDelayRepayJourneyPosted,TravelDisruptionLLMService,TravelForesightInference,TravelForesightService,TravelPolicyCreated,TravelPolicyEvaluationResults,TravelPolicyOverrideItineraryReasonCreated,TravelPolicyUpdated,TravelServiceCancellationStatusChanged,TravelServiceFullyReinstated,TravelServiceRealTimeStatusChanged,TrenItaliaPicoDomestic,TrenItaliaPicoInternational,Ufac-AccessTokenCreated,Ufac-ExchangeTokenCreated,UkCardTypeReferenceDataUpdated,UkCarrierReferenceDataUpdated,UKExtraTypeReferenceDataUpdated,UkFareCategoryReferenceDataUpdated,UkFareDiscountRailReferenceDataUpdated,UKFareSearchExecuted,UkFareTypeReferenceDataUpdated,UKJourneySearchSplitSaveData,UkLocationReferenceDataUpdated,UkPassengerTypeReferenceDataUpdated,UkRouteRestrictionReferenceDataUpdated,UKSeasonFareSearchExecuted,UkTransportModeReferenceDataUpdated,UkTravelClassReferenceDataUpdated,VendorCallsStatistics,VoidableStatusEvent,VoidContextEvent,VoidFailure,WalkUpBuyRecommenderMlService,WebloyaltyCommercialRates,WebloyaltyDailyValidJoins,WebloyaltyMonthlyIntlReport,WebloyaltyWeeklyIntlReport,WebloyaltyWeeklyReport,ZeroAmountAuthorisationFailed,ZeroAmountAuthorisationRequested,ZeroAmountAuthorisationSuccessful"
        }).split(",").collect();

        let mut schema_topics = vec![(
            TopicConfig {
                decoder: RecordDecoder::JsonSchemaDecoder,
                router: RouterStrategy::TopicVersion,
            },
            topics,
        )];

        let mut dlq_topics = vec![(
            TopicConfig {
                decoder: RecordDecoder::JsonStringDecoder,
                router: RouterStrategy::Dlq,
            },
            vec!["BifrostBatchDlq", "BifrostDlq"],
        )];

        schema_topics.append(&mut dlq_topics);

        schema_topics
            .iter()
            .map(|(config, topics)| {
                (
                    *config,
                    topics
                        .iter()
                        .map(|&topic| TopicName(Rc::from(String::from(topic))))
                        .collect(),
                )
            })
            .collect()
    };

    let consumer_properties: Vec<(String, String)> = {
        let mut static_properties: Vec<(String, String)> = [
            ("group.protocol", "classic"),
            ("auto.offset.reset", "latest"),
            ("enable.auto.offset.store", "false"),
            ("enable.auto.commit", "false"),
            ("socket.keepalive.enable", "true"),
            ("security.protocol", "sasl_ssl"),
            ("sasl.mechanism", "OAUTHBEARER"),
            ("ssl.ca.location", "/etc/ssl/certs/ca-certificates.crt"),
            ("debug", "consumer,broker,security,protocol"),
        ]
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();

        let mut dynamic_consumer_properties: Vec<(String, String)> = vec![
            ("group.id".to_string(), get_env_var_or_panic("GROUP_ID")),
            (
                "bootstrap.servers".to_string(),
                get_env_var_or_panic("BOOTSTRAP_SERVERS"),
            ),
            (
                "fetch.max.bytes".to_string(),
                get_env_var_or_panic("FETCH_MAX_BYTES"),
            ),
            (
                "max.partition.fetch.bytes".to_string(),
                get_env_var_or_panic("MAX_PARTITION_FETCH_BYTES"),
            ),
            (
                "receive.message.max.bytes".to_string(),
                get_env_var_or_panic("RECEIVE_MESSAGE_MAX_BYTES"),
            ),
            (
                "queued.min.messages".to_string(),
                get_env_var_or_panic("QUEUED_MIN_MESSAGES"),
            ),
            (
                "queued.max.messages.kbytes".to_string(),
                get_env_var_or_panic("QUEUED_MAX_MESSAGES_KBYTES"),
            ),
            (
                "statistics.interval.ms".to_string(),
                get_env_var_or_panic("KAFKA_CLIENT_STATISTICS_INTERVAL_MS"),
            ),
            (
                "client.rack".to_string(),
                get_env_var_or_panic("CLIENT_RACK"),
            ),
        ];

        static_properties.append(&mut dynamic_consumer_properties);

        static_properties
    };

    let kafka_config = KafkaConfig {
        input_topics,
        consumer_properties,
        principal_name: get_env_var_or_panic("GROUP_ID"),
        region: aws_config::Region::new(get_env_var_or_default("REGION", || {
            "eu-west-1".to_string()
        })),
    };

    let timers_config = TimersConfig {
        commit_tick_ms: get_u64_env_var_or_panic("COMMIT_TICK_MS"),
    };

    let files_config = FileConfig {
        scratch_directory: get_env_var_or_panic("SCRATCH_DIRECTORY").into(),
        target_file_size_b: get_u64_env_var_or_panic("TARGET_FILE_SIZE_B"),
        compression_level: get_u64_env_var_or_panic("COMPRESSION_LEVEL")
            .try_into()
            .unwrap(),
    };

    let upload_config = UploadConfig {
        bucket: get_env_var_or_panic("BUCKET"),
        max_uploads_retry: get_u64_env_var_or_panic("MAX_UPLOADS_RETRY"),
        max_concurrent_uploads: get_u64_env_var_or_panic("MAX_CONCURRENT_UPLOADS"),
        max_active_file_timeout_ms: get_u64_env_var_or_panic("MAX_ACTIVE_FILE_TIMEOUT_MS"),
    };

    SinkConfig {
        kafka: kafka_config,
        files: files_config,
        timers: timers_config,
        uploads: upload_config,
    }
}

fn init_logging() {
    use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .json()
        .init();
}

fn main() {
    init_logging();

    info!("initializing SinkConfig");
    let config = get_config();

    std::fs::create_dir_all(config.files.scratch_directory.as_path())
        .expect("failed to create scratch directory");

    info!("initializing DiskFileRegistry");
    let file_registry = DiskFileRegistry::new(
        &config.files.scratch_directory,
        config.files.compression_level,
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("could not build Tokio runtime");

    runtime.block_on(async {
        info!("initializing S3Upload");
        let uploader = S3Upload::new(
            config.kafka.region.clone(),
            config.uploads.bucket.clone(),
            config.uploads.max_uploads_retry,
            None,
            None,
            None,
            None,
        )
        .await;

        match Sink::start(&config, uploader, file_registry).await {
            Ok(_) => info!("sink event loop exited"),
            Err(error) => error!("sink error: {:?}", error),
        }
    });
}
