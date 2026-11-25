i Developers at Dream Labs JR, 

Thank you for contacting Google Play Developer Support regarding Trade Alert (com.dreamlabs.tradealert).

While we do not offer a pre-compliance check service and cannot pre-approve a specific implementation prior to review, here is how our Full-Screen Intent Permission policy applies to your questions:


Qualification for USE_FULL_SCREEN_INTENT:
For apps targeting Android 14 (API level 34) and above, USE_FULL_SCREEN_INTENT is only automatically granted if the app's core functionality (its primary purpose) is setting an alarm (such as a dedicated alarm clock or timer app) or receiving phone/video calls. Because Trade Alert's core functionality is financial/trading price alerts rather than a dedicated alarm app, it does not qualify for default auto-granting.

Declaring under the "Other" category:
Selecting "Other" (or an ineligible category) in the Play Console declaration will not qualify your app for automatic granting of USE_FULL_SCREEN_INTENT. Your declaration must always accurately reflect your app's core functionality.
Alternative compliant implementation:
Apps that do not qualify for auto-granting can either:

Request user permission manually: Do not claim an auto-grant category. Instead, prompt users to grant access manually (via ACTION_MANAGE_APP_USE_FULL_SCREEN_INTENT in Special App Access settings), check NotificationManager.canUseFullScreenIntent(), and gracefully degrade the experience (for example, falling back to high-priority notifications) if the user declines.
Use standard notifications: Remove USE_FULL_SCREEN_INTENT from your AndroidManifest.xml across all tracks (production and testing) and rely on standard high-priority notifications.
For full technical and policy requirements, please review our guides on Understanding foreground service and full-screen intent requirements and Android full-screen intent limits.

Once your update is ready, please go ahead and submit your app for review. Our team will perform a thorough check and let you know directly if any updates are needed. The standard review process typically takes up to 7 days, depending on submission volume and app complexity.

Thank you for your patience and understanding.
