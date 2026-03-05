import { Authenticator, useAuthenticator } from "@aws-amplify/ui-react";
import Dashboard from "./Dashboard";

function AuthenticatedApp() {
  const { user, signOut } = useAuthenticator();
  return (
    <>
      <header>
        <h1>Analytics</h1>
        <div className="header-right">
          <span className="user-email">
            {user?.signInDetails?.loginId ?? user?.username}
          </span>
          <button className="btn-logout" onClick={signOut}>
            Sign Out
          </button>
        </div>
      </header>
      <Dashboard />
    </>
  );
}

export default function App() {
  return (
    <Authenticator hideSignUp>
      <AuthenticatedApp />
    </Authenticator>
  );
}
