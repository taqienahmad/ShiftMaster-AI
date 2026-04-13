import React, { useState, useEffect } from 'react';
import { 
  auth, 
  db, 
  googleProvider, 
  signInWithPopup, 
  signOut, 
  onAuthStateChanged, 
  collection, 
  onSnapshot, 
  query, 
  orderBy,
  User,
  doc,
  getDoc
} from './lib/firebase';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Toaster } from '@/components/ui/sonner';
import { toast } from 'sonner';
import { 
  Users, 
  Calendar, 
  TrendingUp, 
  LogOut, 
  LayoutDashboard,
  Plus,
  Clock,
  ShieldCheck
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';

// Components
import EmployeeList from './components/EmployeeList';
import ForecastView from './components/ForecastView';
import ErlangForecaster from './components/ErlangForecaster';
import ShiftCodeManager from './components/ShiftCodeManager';
import ForecastVolume from './components/ForecastVolume';
import RosterView from './components/RosterView';
import WorkingDaysReference from './components/WorkingDaysReference';

export default function App() {
  const [user, setUser] = useState<User | null>(null);
  const [isAdmin, setIsAdmin] = useState(false);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('forecast-volume');

  useEffect(() => {
    const handleHashChange = () => {
      const hash = window.location.hash.replace('#', '');
      if (hash && ['forecast-volume', 'shift-codes', 'employees', 'forecast', 'roster', 'working-days'].includes(hash)) {
        setActiveTab(hash);
      }
    };
    window.addEventListener('hashchange', handleHashChange);
    handleHashChange(); // Check on mount
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, async (user) => {
      setUser(user);
      if (user) {
        // Check admin status
        const userDoc = await getDoc(doc(db, 'users', user.uid));
        const userData = userDoc.data();
        setIsAdmin(userData?.role === 'admin' || user.email === 'taqienahmad@gmail.com');
      } else {
        setIsAdmin(false);
      }
      setLoading(false);
    });
    return () => unsubscribe();
  }, []);

  const handleLogin = async () => {
    try {
      await signInWithPopup(auth, googleProvider);
      toast.success('Successfully logged in!');
    } catch (error) {
      console.error(error);
      toast.error('Failed to login');
    }
  };

  const handleLogout = async () => {
    try {
      await signOut(auth);
      toast.success('Logged out');
    } catch (error) {
      console.error(error);
      toast.error('Failed to logout');
    }
  };

  if (loading) {
    return (
      <div className="h-screen w-screen flex items-center justify-center bg-slate-50">
        <motion.div 
          animate={{ rotate: 360 }}
          transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
          className="w-12 h-12 border-4 border-primary border-t-transparent rounded-full"
        />
      </div>
    );
  }

  if (!user) {
    return (
      <div className="min-h-screen bg-slate-50 flex flex-col items-center justify-center p-4">
        <motion.div 
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="max-w-md w-full"
        >
          <Card className="border-none shadow-xl bg-white/80 backdrop-blur-sm">
            <CardHeader className="text-center space-y-4">
              <div className="mx-auto w-16 h-16 bg-primary/10 rounded-2xl flex items-center justify-center">
                <Clock className="w-8 h-8 text-primary" />
              </div>
              <div>
                <div className="flex items-center justify-center gap-2 mb-2">
                  <Clock className="w-8 h-8 text-primary" />
                  <span className="text-3xl font-bold tracking-tight bg-gradient-to-r from-primary to-indigo-600 bg-clip-text text-transparent">TransRosterAI</span>
                </div>
                <CardDescription className="text-slate-500 text-base">
                  Intelligent employee forecasting and scheduling for modern teams.
                </CardDescription>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              <Button onClick={handleLogin} className="w-full h-12 text-lg font-medium" size="lg">
                Sign in with Google
              </Button>
              <p className="text-xs text-center text-slate-400">
                Secure authentication powered by Firebase
              </p>
            </CardContent>
          </Card>
        </motion.div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-50 flex">
      {/* Sidebar */}
      <aside className="w-64 bg-white border-r border-slate-200 flex flex-col hidden md:flex">
        <div className="p-6 flex items-center gap-3">
          <div className="w-10 h-10 bg-indigo-600 rounded-xl flex items-center justify-center shadow-lg shadow-indigo-200">
            <Clock className="w-6 h-6 text-white" />
          </div>
          <div className="flex flex-col">
            <span className="font-bold text-lg tracking-tight text-slate-900">TransRosterAI</span>
            <span className="text-[10px] font-bold text-indigo-500 uppercase tracking-widest">Enterprise</span>
          </div>
        </div>

        <nav className="flex-1 px-4 space-y-1">
          <SidebarItem 
            icon={<TrendingUp className="w-5 h-5" />} 
            label="Forecast Volume" 
            active={activeTab === 'forecast-volume'} 
            onClick={() => { setActiveTab('forecast-volume'); window.location.hash = 'forecast-volume'; }} 
          />
          <SidebarItem 
            icon={<Clock className="w-5 h-5" />} 
            label="Shift Codes" 
            active={activeTab === 'shift-codes'} 
            onClick={() => { setActiveTab('shift-codes'); window.location.hash = 'shift-codes'; }} 
          />
          <SidebarItem 
            icon={<Users className="w-5 h-5" />} 
            label="Employees" 
            active={activeTab === 'employees'} 
            onClick={() => { setActiveTab('employees'); window.location.hash = 'employees'; }} 
          />
          <SidebarItem 
            icon={<Calendar className="w-5 h-5" />} 
            label="Working Days Ref" 
            active={activeTab === 'working-days'} 
            onClick={() => { setActiveTab('working-days'); window.location.hash = 'working-days'; }} 
          />
          <SidebarItem 
            icon={<TrendingUp className="w-5 h-5" />} 
            label="AI Forecast" 
            active={activeTab === 'forecast'} 
            onClick={() => { setActiveTab('forecast'); window.location.hash = 'forecast'; }} 
          />
          <SidebarItem 
            icon={<Calendar className="w-5 h-5" />} 
            label="Roster" 
            active={activeTab === 'roster'} 
            onClick={() => { setActiveTab('roster'); window.location.hash = 'roster'; }} 
          />
        </nav>

        <div className="p-4 border-t border-slate-100">
          <div className="flex items-center gap-3 px-2 py-3 mb-2">
            <div className="w-8 h-8 rounded-full bg-slate-100 flex items-center justify-center overflow-hidden">
              {user.photoURL ? (
                <img src={user.photoURL} alt={user.displayName || ''} referrerPolicy="no-referrer" />
              ) : (
                <Users className="w-4 h-4 text-slate-400" />
              )}
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium text-slate-900 truncate">{user.displayName}</p>
              <p className="text-xs text-slate-500 truncate">{isAdmin ? 'Administrator' : 'Staff'}</p>
            </div>
          </div>
          <Button variant="ghost" className="w-full justify-start text-slate-500 hover:text-destructive" onClick={handleLogout}>
            <LogOut className="w-4 h-4 mr-2" />
            Logout
          </Button>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 flex flex-col overflow-hidden">
        <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-8 md:hidden">
           <div className="flex items-center gap-2">
            <Clock className="w-6 h-6 text-primary" />
            <span className="font-bold text-lg">TransRosterAI</span>
          </div>
          <Button variant="ghost" size="icon" onClick={handleLogout}>
            <LogOut className="w-5 h-5" />
          </Button>
        </header>

        <div className="flex-1 overflow-y-auto p-8">
          <div className={activeTab === 'forecast-volume' ? 'block' : 'hidden'}>
            <ForecastVolume isAdmin={isAdmin} />
          </div>
          <div className={activeTab === 'shift-codes' ? 'block' : 'hidden'}>
            <ShiftCodeManager isAdmin={isAdmin} />
          </div>
          <div className={activeTab === 'employees' ? 'block' : 'hidden'}>
            <EmployeeList isAdmin={isAdmin} />
          </div>
          <div className={activeTab === 'working-days' ? 'block' : 'hidden'}>
            <WorkingDaysReference isAdmin={isAdmin} />
          </div>
          <div className={activeTab === 'forecast' ? 'block' : 'hidden'}>
            <ForecastView isAdmin={isAdmin} />
          </div>
          <div className={activeTab === 'roster' ? 'block' : 'hidden'}>
            <RosterView isAdmin={isAdmin} />
          </div>
        </div>
      </main>
      <Toaster position="top-right" />
    </div>
  );
}

function SidebarItem({ icon, label, active, onClick }: { icon: React.ReactNode, label: string, active: boolean, onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-200 ${
        active 
          ? 'bg-primary text-white shadow-lg shadow-primary/20' 
          : 'text-slate-500 hover:bg-slate-50 hover:text-slate-900'
      }`}
    >
      {icon}
      <span className="font-medium">{label}</span>
    </button>
  );
}
