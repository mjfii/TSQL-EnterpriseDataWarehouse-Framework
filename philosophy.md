## An Enterprise Data Warehouse (“EDW”) Philosophy ##

Generally speaking, there are several philosophies in constructing an EDW; however, the design elements defined in this architecture are governed by the below guiding principles and associated traits.  It is important to note that each of the traits may not apply to every aspect of the EDW, but rather, they will be accounted for in terms of the solution as a whole.

**Information is always motion…**

- **Non-Volatile:**  As information ebbs and flows, all changes are captured and recorded in a common and logical manner.


- **Non-Destructive:**  Depending on the business rules defined in source systems, data can be deleted, removed, or archived.  Instead of persisting the removal downstream, data is identified as being in an active state, or not.


- **Ever-growing:**  Information will always continue to grow in size, so the technology and design can scale to handle incremental growth.


- **Ever-changing:**  As new requirements are envisioned, content can expand without significant redesign and with a limited effort.


- **Variable Latencies:**  Information moves at different rates and from a variety of technologies, the solution accommodates and accounts for this.

**Information is diverse…**

- **Integrated:**  Common data elements exist across many source systems, i.e. Customers.  The solution conforms these elements.

- **Cleansed and/or Profiled Data:**  Not all information is in an acceptable format for reporting and analytics.  The solution manages cleaned and profiled information, and it does not act as a ‘data dumping ground’.
 
- **Homogeneous:**  Since common data elements are potentially stored differently in their original states, the solution homogenizes that information with shared data types and a common taxonomy.

- **Defined Metadata:**  The solution maintains data points about the information itself, including its source, content description, velocity, etc.

**Information is consumed by the business…**

- **Context and Subject Oriented:**  Information can, at times, be difficult to decipher.  The solution provides focused content, with the addition of context and the removal of confusion, allowing for an easily accessible platform to answering business questions.


- **Comprehensive Scope:**  The solution is ready and capable to handle all aspects of the enterprise reporting and analytical needs, including new departments, acquisitions, and new business applications.


- **SLA Management:**  Numerous user (including service accounts) will have the need read and write information, some of which will be far heavier and more complex than others.  The solution will define, support and enforce a strategy to manage service levels.

**Information is valuable…**

- **Validated:**  The solution will go through thorough testing and validation in order to ensure accuracy in the information.  In turn, the solution will garner trust with the business.

- **Securitized:**  The solution will support and maintain a robust security model that allows access to users with valid credentials for any given piece of information, and log attempts by users without clearance.


- **Continuity:**  The solution will maintain traditional backups and more modern high availability technologies in an effort to prevent catastrophic loss and limit downtime, respectively.

8/6/2014 2:57:59 PM 