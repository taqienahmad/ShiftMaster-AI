import React, { useState, useEffect } from 'react';
import { db, collection, onSnapshot, query, orderBy, setDoc, doc, deleteDoc, OperationType, handleFirestoreError } from '../lib/firebase';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from '@/components/ui/dialog';
import { Badge } from '@/components/ui/badge';
import { Users, UserPlus, Search, Trash2, Edit2, Upload, Download, FileSpreadsheet } from 'lucide-react';
import { toast } from 'sonner';

interface Employee {
  id: string;
  nip: string;
  name: string;
  email: string;
  skill: string;
  channel: string;
  site: string;
  gender: string;
  religion: string;
  role: string;
  department: string;
  skills: string[];
}

export default function EmployeeList({ isAdmin }: { isAdmin: boolean }) {
  const [employees, setEmployees] = useState<Employee[]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [isAddDialogOpen, setIsAddDialogOpen] = useState(false);
  const [newEmployee, setNewEmployee] = useState<Partial<Employee>>({
    nip: '',
    name: '',
    email: '',
    skill: '',
    channel: '',
    site: '',
    gender: 'L',
    religion: '',
    role: '',
    department: '',
    skills: []
  });

  useEffect(() => {
    const q = query(collection(db, 'employees'), orderBy('name'));
    const unsubscribe = onSnapshot(q, (snapshot) => {
      const list = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Employee));
      setEmployees(list);
    }, (error) => {
      handleFirestoreError(error, OperationType.LIST, 'employees');
    });
    return () => unsubscribe();
  }, []);

  const downloadTemplate = () => {
    const csvContent = "sep=;\nNo;NIP;Name;Skill;Channel;Site;Gender;Religion\n1;6011450;Abu Sofyan;MPBK;Call;Semarang;L;Islam\n2;6012118;Achmad Luthfi Zakiya;PBK;Call;Semarang;L;Islam";
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'employee_template.csv';
    a.click();
    window.URL.revokeObjectURL(url);
  };

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !isAdmin) return;

    const reader = new FileReader();
    reader.onload = async (event) => {
      try {
        let text = event.target?.result as string;
        // Remove BOM if present
        text = text.replace(/^\ufeff/, '');
        
        let lines = text.split(/\r?\n/).filter(line => line.trim() !== '');
        if (lines.length === 0) return;

        // Skip Excel separator hint if present (e.g., "sep=,")
        if (lines[0].startsWith('sep=')) {
          lines = lines.slice(1);
        }

        if (lines.length < 2) {
          toast.error('CSV file is empty or missing data');
          return;
        }

        // Detect separator (comma, semicolon, or tab)
        const firstLine = lines[0];
        const counts = {
          ',': (firstLine.match(/,/g) || []).length,
          ';': (firstLine.match(/;/g) || []).length,
          '\t': (firstLine.match(/\t/g) || []).length
        };
        const separator = (Object.keys(counts) as Array<keyof typeof counts>).reduce((a, b) => counts[a] > counts[b] ? a : b);
        
        const headers = firstLine.split(separator).map(h => h.trim().toLowerCase());
        
        // Flexible header matching
        const requiredHeaders = ['nip', 'name', 'skill'];
        const missingHeaders = requiredHeaders.filter(req => !headers.some(h => h.includes(req)));
        
        if (missingHeaders.length > 0) {
          toast.error(`Missing required columns: ${missingHeaders.join(', ')}`);
          console.log('Detected headers:', headers);
          return;
        }

        let successCount = 0;
        let errorCount = 0;

        for (let i = 1; i < lines.length; i++) {
          const values = lines[i].split(separator).map(v => v.trim());
          const empData: any = {};
          
          headers.forEach((header, index) => {
            const mapping: any = {
              'nip': 'nip',
              'name': 'name',
              'skill': 'skill',
              'channel': 'channel',
              'site': 'site',
              'gender': 'gender',
              'religion': 'religion'
            };
            // Find mapping that is contained in the header
            const matchedKey = Object.keys(mapping).find(key => header.includes(key));
            if (matchedKey) {
              empData[mapping[matchedKey]] = values[index];
            }
          });

          if (empData.name && empData.nip && empData.skill) {
            try {
              const id = empData.nip || Math.random().toString(36).substring(2, 9);
              await setDoc(doc(db, 'employees', id), { 
                ...empData, 
                id, 
                email: empData.email || '',
                role: empData.role || empData.skill,
                department: empData.department || empData.site || '',
                skills: [] 
              });
              successCount++;
            } catch (err) {
              console.error('Error uploading employee:', err);
              errorCount++;
            }
          } else {
            errorCount++;
          }
        }
        
        if (successCount > 0) {
          toast.success(`Successfully uploaded ${successCount} employees`);
        }
        if (errorCount > 0) {
          toast.error(`Failed to upload ${errorCount} rows. Check format.`);
        }
      } catch (err) {
        toast.error('Failed to process CSV file');
      }
    };
    reader.readAsText(file);
    // Reset input
    e.target.value = '';
  };

  const handleAddEmployee = async () => {
    if (!newEmployee.name || !newEmployee.nip || !newEmployee.skill) {
      toast.error('Please fill in required fields (Name, NIP, Skill)');
      return;
    }

    try {
      const id = newEmployee.nip || Math.random().toString(36).substring(2, 9);
      await setDoc(doc(db, 'employees', id), {
        ...newEmployee,
        id,
        email: newEmployee.email || '',
        role: newEmployee.role || newEmployee.skill || '',
        department: newEmployee.department || newEmployee.site || '',
        skills: []
      });
      toast.success('Employee added successfully');
      setIsAddDialogOpen(false);
      setNewEmployee({ 
        nip: '', name: '', email: '', skill: '', channel: '', 
        site: '', gender: 'L', religion: '', role: '', 
        department: '', skills: [] 
      });
    } catch (error) {
      handleFirestoreError(error, OperationType.CREATE, 'employees');
    }
  };

  const handleDeleteEmployee = async (id: string) => {
    try {
      await deleteDoc(doc(db, 'employees', id));
      toast.success('Employee deleted');
    } catch (error) {
      handleFirestoreError(error, OperationType.DELETE, 'employees');
    }
  };

  const [isClearDialogOpen, setIsClearDialogOpen] = useState(false);

  const handleClearAll = async () => {
    if (!isAdmin) return;
    try {
      const promises = employees.map(emp => deleteDoc(doc(db, 'employees', emp.id)));
      await Promise.all(promises);
      toast.success('All employees cleared');
      setIsClearDialogOpen(false);
    } catch (error) {
      toast.error('Failed to clear employees');
    }
  };

  const filteredEmployees = employees.filter(emp => 
    emp.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    emp.email.toLowerCase().includes(searchTerm.toLowerCase()) ||
    emp.role.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-slate-900">Employees</h1>
          <p className="text-slate-500">Manage your team members and their roles.</p>
        </div>
        
        {isAdmin && (
          <div className="flex items-center gap-2">
            <Dialog open={isClearDialogOpen} onOpenChange={setIsClearDialogOpen}>
              <DialogTrigger asChild>
                <Button 
                  variant="destructive" 
                  className="gap-2"
                  disabled={employees.length === 0}
                >
                  <Trash2 className="w-4 h-4" />
                  Clear Data
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Are you absolutely sure?</DialogTitle>
                  <p className="text-sm text-slate-500 py-4">
                    This action will permanently delete all employee records from the database. This cannot be undone.
                  </p>
                </DialogHeader>
                <DialogFooter>
                  <Button variant="outline" onClick={() => setIsClearDialogOpen(false)}>Cancel</Button>
                  <Button variant="destructive" onClick={handleClearAll}>Delete All Data</Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
            <Button variant="outline" onClick={downloadTemplate} className="gap-2">
              <Download className="w-4 h-4" />
              Template
            </Button>
            
            <label className="cursor-pointer">
              <Input type="file" accept=".csv" className="hidden" onChange={handleFileUpload} />
              <div className="flex items-center gap-2 px-4 py-2 rounded-md bg-slate-100 hover:bg-slate-200 text-sm font-medium transition-colors">
                <Upload className="w-4 h-4" />
                Upload CSV
              </div>
            </label>
            <Dialog open={isAddDialogOpen} onOpenChange={setIsAddDialogOpen}>
              <DialogTrigger asChild>
                <Button className="gap-2">
                  <UserPlus className="w-4 h-4" />
                  Add Employee
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Add New Employee</DialogTitle>
                </DialogHeader>
                <div className="grid grid-cols-2 gap-4 py-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">NIP</label>
                    <Input 
                      placeholder="112" 
                      value={newEmployee.nip} 
                      onChange={e => setNewEmployee({...newEmployee, nip: e.target.value})} 
                    />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Full Name</label>
                    <Input 
                      placeholder="Abu Sofyan" 
                      value={newEmployee.name} 
                      onChange={e => setNewEmployee({...newEmployee, name: e.target.value})} 
                    />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Skill</label>
                    <Input 
                      placeholder="MPBK" 
                      value={newEmployee.skill} 
                      onChange={e => setNewEmployee({...newEmployee, skill: e.target.value})} 
                    />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Channel</label>
                    <Input 
                      placeholder="Call" 
                      value={newEmployee.channel} 
                      onChange={e => setNewEmployee({...newEmployee, channel: e.target.value})} 
                    />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Site</label>
                    <Input 
                      placeholder="Semarang" 
                      value={newEmployee.site} 
                      onChange={e => setNewEmployee({...newEmployee, site: e.target.value})} 
                    />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Gender</label>
                    <select 
                      className="w-full h-10 px-3 rounded-md border border-input bg-background text-sm ring-offset-background focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
                      value={newEmployee.gender}
                      onChange={e => setNewEmployee({...newEmployee, gender: e.target.value})}
                    >
                      <option value="L">L (Male)</option>
                      <option value="P">P (Female)</option>
                    </select>
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Religion</label>
                    <Input 
                      placeholder="Islam" 
                      value={newEmployee.religion} 
                      onChange={e => setNewEmployee({...newEmployee, religion: e.target.value})} 
                    />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Email (Optional)</label>
                    <Input 
                      type="email" 
                      placeholder="john@example.com" 
                      value={newEmployee.email} 
                      onChange={e => setNewEmployee({...newEmployee, email: e.target.value})} 
                    />
                  </div>
                </div>
                <DialogFooter>
                  <Button variant="outline" onClick={() => setIsAddDialogOpen(false)}>Cancel</Button>
                  <Button onClick={handleAddEmployee}>Save Employee</Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>
        )}
      </div>

      <Card className="border-none shadow-sm overflow-hidden">
        <CardHeader className="bg-white border-b border-slate-100 py-4">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <Input 
              placeholder="Search employees..." 
              className="pl-10 bg-slate-50 border-none"
              value={searchTerm}
              onChange={e => setSearchTerm(e.target.value)}
            />
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader className="bg-slate-50/50">
              <TableRow>
                <TableHead>NIP</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Skill</TableHead>
                <TableHead>Channel</TableHead>
                <TableHead>Site</TableHead>
                <TableHead>Gender</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredEmployees.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} className="h-32 text-center text-slate-500">
                    No employees found.
                  </TableCell>
                </TableRow>
              ) : (
                filteredEmployees.map((emp) => (
                  <TableRow key={emp.id} className="hover:bg-slate-50/50 transition-colors">
                    <TableCell className="font-mono text-xs">{emp.nip}</TableCell>
                    <TableCell>
                      <div className="flex flex-col">
                        <span className="font-medium text-slate-900">{emp.name}</span>
                        <span className="text-xs text-slate-500">{emp.religion}</span>
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge variant="secondary" className="font-normal bg-blue-50 text-blue-700 border-blue-100">
                        {emp.skill}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-slate-600 text-sm">{emp.channel || '-'}</TableCell>
                    <TableCell className="text-slate-600 text-sm">{emp.site || '-'}</TableCell>
                    <TableCell className="text-slate-600 text-sm">{emp.gender || '-'}</TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-2">
                        {isAdmin && (
                          <Button 
                            variant="ghost" 
                            size="icon" 
                            className="text-slate-400 hover:text-destructive"
                            onClick={() => handleDeleteEmployee(emp.id)}
                          >
                            <Trash2 className="w-4 h-4" />
                          </Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
