# Option Engine (multi-user) + Android APK

## 1. Server deploy (Render)
1. Repo GitHub par push karo.
2. render.com -> New -> Blueprint -> is repo ko chuno (render.yaml apne aap padh lega).
3. Deploy hone par URL milega, jaise `https://option-engine-xxxx.onrender.com`.

## 2. APK build (GitHub Actions)
1. Repo -> Settings -> Secrets and variables -> Actions -> **Variables** -> New:
   `SERVER_URL` = apna Render URL (bina `/` ke ant mein).
2. Actions tab -> "Build APK" -> Run workflow.
3. Khatam hone par Artifacts se `option-engine-apk` download karo, zip kholo, `app-debug.apk` phone mein install karo.

## 3. Har user ke liye (aap aur dost)
1. developer console (account.upstox.com/developer/apps) par apna app banao.
2. Redirect URI mein wahi daalo jo app ke login screen par dikhta hai (`<server>/callback`).
3. App kholo -> API Key + Secret daalo -> Login with Upstox. Roz subah ek baar.

`upstox_config.json` kabhi GitHub par mat daalo (.gitignore mein hai).
